%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------
%%
%% @doc Test Yokozuna's map/reduce integration.
-module(yz_mapreduce).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-include("yokozuna.hrl").

-type host() :: string().
-type portnum() :: integer().

-define(CFG,
        [{riak_core,
          [
           {ring_creation_size, 16}
          ]},
         {riak_kv,
          [
           %% make handoff happen faster
           {handoff_concurrency, 16},
           {inactivity_timeout, 2000}
          ]},
         {yokozuna,
          [
	   {enabled, true}
          ]}
        ]).

-spec confirm() -> pass.
confirm() ->
    Index = <<"mr_index">>,
    Bucket = {Index, <<"b1">>},
    random:seed(now()),
    Cluster = rt:build_cluster(4, ?CFG),
    rt:wait_for_cluster_service(Cluster, yokozuna),
    Conns = lists:zip(Cluster, yz_rt:open_pb_conns(Cluster)),
    lager:info("CONNS: ~p", [Conns]),
    yz_rt:create_index(yz_rt:select_random(Cluster), Index),
    yz_rt:wait_for_index(Cluster, Index),
    yz_rt:set_bucket_type_index(yz_rt:select_random(Cluster), Index),
    timer:sleep(500),
    write_100_objs(Cluster, Bucket),
    _ = verify_100_objs_mr(http, Cluster, Index),
    _ = add_500_intercept(Cluster),
    _ = verify_500_fail(http, Cluster, Index),
    _ = verify_500_fail(pb, Conns, Index),
    pass.

-spec add_500_intercept(list()) -> ok.
add_500_intercept(Cluster) ->
    _ = [rt_intercept:add(Node, {yz_solr, [{{dist_search, 3}, dist_search_500}]})
         || Node <- Cluster],
    ok.

make_mr(http, Index) ->
    MakeTick = [{map, [{language, <<"javascript">>},
                       {keep, false},
                       {source, <<"function(v) { return [1]; }">>}]}],
    ReduceSum = [{reduce, [{language, <<"javascript">>},
                           {keep, true},
                           {name, <<"Riak.reduceSum">>}]}],
    [{inputs, [{module, <<"yokozuna">>},
               {function, <<"mapred_search">>},
               {arg, [Index, <<"name_s:yokozuna">>]}]},
     {'query', [MakeTick, ReduceSum]}];
make_mr(pb, Index) ->
    Inputs = {modfun, yokozuna, mapred_search, [Index, <<"name_s:yokozuna">>]},
    TickFun = fun(_, _, _) -> [1] end,
    MakeTick = {map, {qfun, TickFun}, none, false},
    %% MakeTick = {map, {strfun, <<"fun(_) -> 1 end.">>}, none, false},
    ReduceSum = riak_kv_mapreduce:reduce_sum(true),
    Query = [MakeTick, ReduceSum],
    {Inputs, Query}.

pb_mr(Conn, {Inputs, Query}) ->
    riakc_pb_socket:mapred(Conn, Inputs, Query).

-spec verify_100_objs_mr(http | pb, list(), index_name()) -> ok.
verify_100_objs_mr(http, Cluster, Index) ->
    MR = make_mr(http, Index),
    F = fun(Node) ->
                HP = hd(yz_rt:host_entries(rt:connection_info([Node]))),
                A = hd(mochijson2:decode(http_mr(HP, MR))),
                lager:info("Running map-reduce job on ~p", [Node]),
                lager:info("E: 100, A: ~p", [A]),
                100 == A
        end,
    yz_rt:wait_until(Cluster, F).

-spec verify_500_fail(http | pb, list(), index_name()) -> ok.
verify_500_fail(http, Cluster, Index) ->
    MR = make_mr(http, Index),
    F = fun(Node) ->
                HP = hd(yz_rt:host_entries(rt:connection_info([Node]))),
                XX = http_mr(HP, MR),
                lager:info("HTTP MR RESULT: ~p~n", [XX]),
                %% A = hd(mochijson2:decode(XX)),
                %% lager:info("Running HTTP map-reduce job on ~p", [Node]),
                %% lager:info("E: 100, A: ~p", [A]),
                %% 100 == A
                true
        end,
    yz_rt:wait_until(Cluster, F);
verify_500_fail(pb, Conns, Index) ->
    MR = make_mr(pb, Index),
    F = fun() ->
                [begin
                     lager:info("Running PB map-reduce job on ~p", [Node]),
                     XX = pb_mr(Conn, MR),
                     lager:info("~nPB MR RESULT: ~p~n", [XX])
                 end || {Node, Conn} <- Conns],
                %% A = hd(mochijson2:decode(XX)),
                %% lager:info("E: 100, A: ~p", [A]),
                %% 100 == A
                true
        end,
    rt:wait_until(F).

-spec write_100_objs([node()], index_name()) -> ok.
write_100_objs(Cluster, Bucket) ->
    lager:info("Writing 100 objects"),
    lists:foreach(write_obj(Cluster, Bucket), lists:seq(1,100)).

-spec write_obj([node()], bucket()) -> fun().
write_obj(Cluster, Bucket) ->
    fun(N) ->
            PL = [{name_s,<<"yokozuna">>}, {num_i,N}],
            Key = list_to_binary(io_lib:format("key_~B", [N])),
            Body = mochijson2:encode(PL),
            HP = yz_rt:select_random(yz_rt:host_entries(rt:connection_info(Cluster))),
            CT = "application/json",
            lager:info("Writing object with bkey ~p [~p]", [{Bucket, Key}, HP]),
            yz_rt:http_put(HP, Bucket, Key, CT, Body)
    end.

-spec http_mr({host(), portnum()}, term()) -> binary().
http_mr({Host,Port}, MR) ->
    URL = ?FMT("http://~s:~s/mapred", [Host, integer_to_list(Port)]),
    Opts = [],
    Headers = [{"content-type", "application/json"}],
    Body = mochijson2:encode(MR),
    {ok, Code, _, RBody} = ibrowse:send_req(URL, Headers, post, Body, Opts),
    io:format("~n~nCODE: ~s~n~n", [Code]),
    io:format("~n~nBODY: ~p~n~n", [RBody]),
    RBody.
