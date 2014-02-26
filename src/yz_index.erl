%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(yz_index).
-behavior(gen_server).
-include("yokozuna.hrl").
-compile(export_all).
-export([code_change/3,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         init/1,
         terminate/2]).

-record(create_req,
        {
          %% The caller to which a reply should be made.
          from :: term(),

          %% Reference do distinguish delayed replies from previous
          %% failed request.
          reference :: reference(),

          %% List of nodes for which a reply is outstanding.
          outstanding_nodes :: [nodes()],

          %% List of replies received.
          replies :: [{node(), Reply::term()}]
        }).
-type create_req() :: #create_req{}.

-record(state,
        {
          %% Dictionary of outstanding create requests keyed by name
          %% and reference.
          outstanding_create_requests :: dict(index_name(), creat_req())
        }).
-type state() :: #state{}.

-record(index_info,
        {
          %% Each index has it's own N value. This is needed so that
          %% the query plan can be calculated. It is up to the user to
          %% make sure that all buckets associated with an index use
          %% the same N value as the index.
          n_val :: n(),

          %% The name of the schema this index is using.
          schema_name :: schema_name()
        }).
-type index_info() :: #index_info{}.

%% @doc This module contains functionaity for using and administrating
%%      indexes.  In this case an index is an instance of a Solr Core.

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Get the list of buckets associated with `Index'.
-spec associated_buckets(index_name(), ring()) -> [bucket()].
associated_buckets(Index, Ring) ->
    AllProps = riak_core_bucket:get_buckets(Ring),
    Assoc = [riak_core_bucket:name(BProps)
             || BProps <- AllProps,
                proplists:get_value(?YZ_INDEX, BProps, ?YZ_INDEX_TOMBSTONE) == Index],
    case is_default_type_indexed(Index) of
        true -> [Index|Assoc];
        false -> Assoc
    end.

%% @see create/2
-spec create(index_name()) -> ok.
create(Name) ->
    create(Name, ?YZ_DEFAULT_SCHEMA_NAME).

%% @see create/3
-spec create(index_name(), schema_name()) -> ok | {error, schema_not_found}.
create(Name, SchemaName) ->
    DefaultNVal = riak_core_bucket:default_object_nval(),
    create(Name, SchemaName, DefaultNVal).

%% @doc Create the index `Name' across the entire cluster using
%%      `SchemaName' as the schema and `NVal' as the N value.
%%
%% `ok' - The schema was found and index added to the list.
%%
%% `schema_not_found' - The `SchemaName' could not be found.
-spec create(index_name(), schema_name(), n() | undefined) ->
                    ok |
                    {error, schema_not_found}.
create(Name, SchemaName, undefined) ->
    DefaultNVal = riak_core_bucket:default_object_nval(),
    create(Name, SchemaName, DefaultNVal);

create(Name, SchemaName, NVal) when is_integer(NVal),
                                    NVal > 0 ->
    case yz_schema:exists(SchemaName) of
        false ->
            {error, schema_not_found};
        true  ->
            Info = make_info(SchemaName, NVal),
            ok = riak_core_metadata:put(?YZ_META_INDEXES, Name, Info)
    end.

%% @doc Determine if an index exists. For an index to exist it must 1)
%% be written to official index list, 2) have a corresponding index
%% dir in the root dir and 3) respond to a ping indicating it started
%% properly.  If Solr is down then the check will fallback to
%% performing only the first two checks. If they fail then it
%% shouldn't exist in Solr.
-spec exists(index_name()) -> boolean().
exists(Name) ->
    InMeta = riak_core_metadata:get(?YZ_META_INDEXES, Name) /= undefined,
    DiskIndexNames = get_indexes_from_disk(?YZ_ROOT_DIR),
    OnDisk = lists:member(Name, DiskIndexNames),
    case yz_solr:is_up() of
        true ->
            InMeta andalso OnDisk andalso yz_solr:ping(Name);
        false ->
            InMeta andalso OnDisk
    end.

%% @doc Removed the index `Name' from cluster meta.
-spec remove(index_name()) -> ok.
remove(Name) ->
    ok = riak_core_metadata:delete(?YZ_META_INDEXES, Name).

%% @doc Determine list of indexes based on filesystem as opposed to
%% the Riak ring or Solr HTTP resource.
%%
%% NOTE: This function assumes that all Yokozuna indexes live directly
%% under the Yokozuna root data directory and that any dir with a
%% `core.properties' file is an index. DO NOT create a dir with a
%% `core.properties' for any other reason or it will confuse this
%% function and potentially have other consequences up the stack.
-spec get_indexes_from_disk(string()) -> [index_name()].
get_indexes_from_disk(Dir) ->
    Files = filelib:wildcard(filename:join([Dir, "*"])),
    [unicode:characters_to_binary(filename:basename(F))
     || F <- Files,
        filelib:is_dir(F) andalso
            filelib:is_file(filename:join([F, "core.properties"]))].

%% @doc Determine the list of indexes based on the cluster metadata.
-spec get_indexes_from_meta() -> indexes().
get_indexes_from_meta() ->
    riak_core_metadata:fold(fun meta_index_list_acc/2,
                            [], ?YZ_META_INDEXES, [{resolver, lww}]).

-spec get_index_info(index_name()) -> undefined | index_info().
get_index_info(Name) ->
    riak_core_metadata:get(?YZ_META_INDEXES, Name).

%% @doc Get the N value from the index info.
-spec get_n_val(index_info()) -> n().
get_n_val(IndexInfo) ->
    IndexInfo#index_info.n_val.

%% @priavte
%%
%% @doc Create the Solr Core locally.
-spec create_solr_core(index_name(), schema_name(), raw_schema()) ->
                              ok | {error, Reason::term()}.
create_solr_core(Name, RawSchema) ->
    IndexDir = index_dir(Name),
    ConfDir = filename:join([IndexDir, "conf"]),
    ConfFiles = filelib:wildcard(filename:join([?YZ_PRIV, "conf", "*"])),
    DataDir = filename:join([IndexDir, "data"]),
    SchemaPath = filename:join([ConfDir, "schema.xml"]),

    ok = yz_misc:make_dirs([ConfDir, DataDir]),
    ok = yz_misc:copy_files(ConfFiles, ConfDir, update),

    %% Delete `core.properties' file or CREATE may complain
    %% about the core already existing. This can happen when
    %% the core is initially created with a bad schema. Solr
    %% gets in a state where CREATE thinks the core already
    %% exists but RELOAD says no core exists.
    PropsFile = filename:join([IndexDir, "core.properties"]),
    _ = file:delete(PropsFile),

    ok = file:write_file(SchemaPath, RawSchema),

    CoreProps = [
                 {name, Name},
                 {index_dir, IndexDir},
                 {cfg_file, ?YZ_CORE_CFG_FILE},
                ],

    case yz_solr:core(create, CoreProps) of
        {ok, _, _} -> ok;
        {error, Err} = Error -> Error
    end.

%% @doc Delete the solr core locally.
-spec delete_solr_core(index_name()) -> ok | {error, Reason::term()}.
delete_solr_core(Name) ->
    CoreProps = [
                    {core, Name},
                    {delete_instance, "true"}
                ],
    yz_solr:core(remove, CoreProps).

%% @doc Reload the `Index' cluster-wide. By default this will also
%% pull the latest version of the schema associated with the
%% index. This call will block for up 5 seconds. Any node which could
%% not reload its index will be returned in a list of failed nodes.
%%
%% Options:
%%
%%   `{schema, boolean()}' - Whether to reload the schema, defaults to
%%   true.
%%
%%   `{timeout, ms()}' - Timeout in milliseconds.
-spec reload(index_name()) -> {ok, [node()]} | {error, reload_errs()}.
reload(Index) ->
    reload(Index, []).

-spec reload(index_name(), reload_opts()) -> {ok, [node()]} |
                                             {error, reload_errs()}.
reload(Index, Opts) ->
    TO = proplists:get_value(timeout, Opts, 5000),
    {Responses, Down} =
        riak_core_util:rpc_every_member_ann(?MODULE, reload_local, [Index, Opts], TO),
    Down2 = [{Node, {error,down}} || Node <- Down],
    BadResponses = [R || {_,{error,_}}=R <- Responses],
    case Down2 ++ BadResponses of
        [] ->
            Nodes = [Node || {Node,_} <- Responses],
            {ok, Nodes};
        Errors ->
            {error, Errors}
    end.

%% @doc Remove documents in `Index' that are not owned by the local
%%      node.  Return the list of non-owned partitions found.
-spec remove_non_owned_data(index_name(), ring()) -> [p()].
remove_non_owned_data(Index, Ring) ->
    IndexPartitions = yz_cover:reify_partitions(Ring,
                                                yokozuna:partition_list(Index)),
    OwnedAndNext = yz_misc:owned_and_next_partitions(node(), Ring),
    NonOwned = ordsets:subtract(IndexPartitions, OwnedAndNext),
    LNonOwned = yz_cover:logical_partitions(Ring, NonOwned),
    Queries = [{'query', <<?YZ_PN_FIELD_S, ":", (?INT_TO_BIN(LP))/binary>>}
               || LP <- LNonOwned],
    ok = yz_solr:delete(Index, Queries),
    NonOwned.

-spec schema_name(index_info()) -> schema_name().
schema_name(Info) ->
    Info#index_info.schema_name.

-type create_req_opt() :: {n_val, n()} |
                          {schema, schema_name()}.

-type create_req_opts() :: [create_req_opt()].

-type create_req_return() :: ok | {error, Reason::term()}.

-spec create_request(index_name(), create_req_opts()) -> create_req_return().
create_request(Name, Options) when is_binary(Name) ->
    case validate_options(Options) of
        ok ->
            case are_all_nodes_up() of
                true ->
                    case not is_mixed_cluster() of
                        true ->
                            Ring = yz_misc:get_ring(transformed),
                            send_create_request_to_claimant(Ring, Name, Options);
                        false ->
                            {error, mixed_cluster}
                    end;
                false ->
                    {error, not_all_nodes_up}
            end;
        {error, _} = Err ->
            Err
    end.

-spec send_create_request_to_claimant(ring(), index_name(), create_req_opts()) ->
                                             ok | {error, Reason::term()}.
send_create_request_to_claimant(Ring, Name, Options) ->
    Claimant = get_claimant(Ring),
    gen_server:call({yz_index, Claimant}, {create_request, Name, Options}, infinity).

%% @private
%%
%% @doc Extract the `Claimant' from the `Ring'.
-spec get_claimant(ring()) -> Claimant::node().
get_claimant(Ring) ->
    riak_core_ring:claimant(Ring).

%% @private
%%
%% @doc Check if the given `Node' is the claimant according to `Ring'.
-spec is_claimant(ring(), node()) -> boolean().
is_claimant(Ring, Node) ->
    Node == get_claimant(Ring).

%% @private
-spec are_all_nodes_up() -> boolean().
are_all_nodes_up() ->
    [] == riak_core_apl:offline_owners(?YZ_SVC_NAME).

%% @private
%%
%% TODO: implement
is_mixed_cluster() ->
    false.

%% @private
%%
%% @doc Validate the create request options.
-spec validate_options(create_req_opts()) -> ok | {error, Reason::term()}.
validate_options([]) ->
    ok;
validate_options([Opt|Rest]) ->
    case validate_option(Opt) of
        ok -> validate_options(Rest);
        {error, _} = Error -> Error
    end.

%% @private
%%
%% @doc Validate individual request options.
-spec validate_options(create_req_opt()) -> ok | {error, Reason::term()}.
validate_option({n_val, NVal}=Opt) ->
    case is_integer(NVal) andalso NVal > 0 of
        true ->
            ok;
        false ->
            {error, {bad_opt, Opt}}
    end;
validate_option({schema, SchemaName}=Opt) ->
    case is_binary(SchemaName) andalso yz_schema:exists(SchemaName) of
        true ->
            ok;
        false ->
            {error, {bad_opt, Opt}}
    end;
validate_option(Unknown) ->
    {error, {unknown_opt, Unknown}}.


%% @private
%% send_create_request_to_claimant(

%%%===================================================================
%%% Server Callbacks
%%%===================================================================

init([]) ->
    {ok, #state{}}.

handle_cast({create_core, From, Ref, Name, RawSchema}, State) ->
    case create_solr_core(Name,  RawSchema) of
        ok ->
            _ = gen_server:cast(From, {create_success, self(), Ref, Name}),
            {noreply, State};
        {error, _} = Error ->
            _ = gen_server:cast(From, {create_failure, self(), Ref, Name}),
            {noreply, State}
    end;

handle_cast({create_success, Node, Ref, Name}, State) ->
    OutstandingReqs = State#state.outstanding_create_requests,
    case dict:find(Name, OutstandingReqs) of
        {ok, #create_req{} = CR}
handle_cast(Msg, State) ->
    lager:warning("Unknown message ~p", [Msg]),
    {noreply, State}.

handle_call({create_request, Name, Options}, From, State) ->
    lager:debug("Received create request for ~s with options ~p", [Name, Options]),
    OutstandingReqs = State#state.outstanding_create_requests,
    case does_req_already_exist(Name, OutstandingReqs) of
        true ->
            Reply = {error, in_progress},
            {reply, Reply, State};
        false ->
            SchemaName = proplsits:get_value(schema, Options, ?YZ_DEFAULT_SCHEMA_NAME),
            {ok, RawSchema} = yz_schema:get(SchemaName),
            case test_index(RawSchema) of
                ok ->
                    Reference = make_reference(),
                    Nodes = get_yokozuna_nodes(),
                    CreateReq = #create_req{from=From,
                                            ref=Reference,
                                            outstanding_nodes=Nodes,
                                            replies=[]},
                    CoreReq = {create_core, self(), Name, RawSchema},
                    OutstandingReqs2 = dict:store(Name, CoreReq, OutstandingReqs),
                    State2 = State#state{outstanding_create_requests=OutstandingReqs2},
                    _ = gen_server:abcast(Nodes, ?MODULE, Req),
                    _ = erlang:send_after(60 * 1000, self(), {create_timeout, Name, Reference}),
                    {noreply, State2};
                {error, _} = Error ->
                    {reply, Error, State}
            end,
    end;

handle_call(Msg, _From, State) ->
    lager:warning("Unknown message ~p", [Msg]),
    {noreply, State}.

handle_info({create_timeout, Name, Reference}, State) ->
    OutstandingReqs = State#state.outstanding_create_requests,
    case dict:find(Name, OutstandingReqs) of
        {ok, #create_req{} = CR} ->
            case Reference == CR#create_req.reference of
                true ->
                    %% Create request timed out before all nodes replied.
                    OutstandingReqs2 = dict:erase(Name, OutstandingReqs),
                    From = CR#create_req.from,
                    OutstandingNodes = CR#create_req.outstanding_nodes,
                    gen_server:reply(From, {error, {timeout, OutstandingNodes}}),
                    State2 = State#state{outstanding_create_requests=OutstandingReqs2},
                    {noreply, State2};
                false ->
                    %% Timeout for create request which completed
                    {noreply, State}
            end;
        error ->
            %% Timeout for create request which completed
            {noreply, State}
    end.

%% @private
%%
%% NOTE: One day Yokozuna may have it's own ring and only run on
%% subset of nodes.
get_yokozuna_nodes() ->
    Ring = yz_misc:get_ring(transformed),
    riak_core_ring:all_owners(Ring).

%% @private
does_req_already_exist(Name, OutstandingReqs) ->
    dict:is_key(Name, OutstandingReqs).

%% @private
-spec test_index(index_name(), raw_schema()) -> ok | {error, Reason::term()}.
test_index(Name, RawSchema) ->
    TestIndexName = test_index_name(Name),
    case create_solr_core(TestIndexName, RawSchema) of
        ok ->
            case core_healthcheck(TestIndexName) of
                ok ->
                    case delete_solr_core(TestIndexName) of
                        ok ->
                            ok;
                        {error, Reason} ->
                            %% Failed to cleanup test index but index
                            %% was created successfully so return ok
                            lager:warning("Failure to delete test index ~s with reason ~p",
                                          [TestIndexName, Reason]),
                            ok
                    end;
                {error, _} = Error ->
                    Error
            end;
        {error, _} Error ->
            Error
    end.

%% @private
-spec core_healthcheck(index_name()) -> ok | {error, Reason::term()}.
core_healthcheck(Name) ->
    case yz_solr:core(status, [{wt,json}, {core,Name}]) of
        {ok, _, Body} ->
            case kvc:path([<<"initFailures">>], mochijson2:decode(Body)) of
                {struct, []} ->
                    ok;
                {struct, [{Name, InitFailure}]} ->
                    {error, {init_failure, InitFailure}}
            end;
        {error, _} = Error ->
            Error
    end.

%% @private
-spec test_index_name(index_name()) -> index_name().
test_index_name(Name) ->
    <<"_test_",Name/binary>>.

%%%===================================================================
%%% Private
%%%===================================================================

%% @private
%%
%% @doc Used to accumulate list of indexes while folding over index
%% metadata.
-spec meta_index_list_acc({index_name(), '$deleted' | term()}, indexes()) ->
                                 indexes().
meta_index_list_acc({_,'$deleted'}, Acc) ->
    Acc;
meta_index_list_acc({Key,_}, Acc) ->
    [Key|Acc].

%% @private
-spec reload_local(index_name(), reload_opts()) ->
                          ok | {error, term()}.
reload_local(Index, Opts) ->
    TO = proplists:get_value(timeout, Opts, 5000),
    ReloadSchema = proplists:get_value(schema, Opts, true),
    case ReloadSchema of
        true ->
            case reload_schema_local(Index) of
                ok ->
                    case yz_solr:core(reload, [{core, Index}], TO) of
                        {ok,_,_} -> ok;
                        Err -> Err
                    end;
                {error,_}=Err ->
                    Err
            end;
        false ->
            case yz_solr:core(reload, [{core, Index}]) of
                {ok,_,_} -> ok;
                Err -> Err
            end
    end.

%% @private
-spec reload_schema_local(index_name()) -> ok | {error, term()}.
reload_schema_local(Index) ->
    IndexDir = index_dir(Index),
    ConfDir = filename:join([IndexDir, "conf"]),
    SchemaName = schema_name(get_index_info(Index)),
    case yz_schema:get(SchemaName) of
        {ok, RawSchema} ->
            SchemaFile = filename:join([ConfDir, yz_schema:filename(SchemaName)]),
            file:write_file(SchemaFile, RawSchema);
        {error, Reason} ->
            {error, Reason}
    end.

index_dir(Name) ->
    filename:absname(filename:join([?YZ_ROOT_DIR, Name])).

%% @private
%%
%% @doc Determine if the bucket named `Index' under the default
%% bucket-type has `search' property set to `true'. If so this is a
%% legacy Riak Search bucket/index which is associated with a Yokozuna
%% index of the same name.
-spec is_default_type_indexed(index_name()) -> boolean().
is_default_type_indexed(Index) ->
    Props = riak_core_bucket:get_bucket(Index),
    %% Check against `true' atom in case the value is <<"true">> or
    %% "true" which, hopefully, it should not be.
    true == proplists:get_value(search, Props, false).

-spec make_info(binary(), n()) -> index_info().
make_info(SchemaName, NVal) ->
    #index_info{n_val=NVal,
                schema_name=SchemaName}.
