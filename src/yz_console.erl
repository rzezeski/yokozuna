%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 Basho Technologies, Inc.  All Rights Reserved.
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
-module(yz_console).
-include("yokozuna.hrl").
-export([aae_status/1,
         switch_to_new_search/1]).

%% @doc Print the Active Anti-Entropy status to stdout.
-spec aae_status([]) -> ok.
aae_status([]) ->
    ExchangeInfo = yz_kv:compute_exchange_info(),
    riak_kv_console:aae_exchange_status(ExchangeInfo),
    io:format("~n"),
    TreeInfo = yz_kv:compute_tree_info(),
    riak_kv_console:aae_tree_status(TreeInfo),
    io:format("~n"),
    riak_kv_console:aae_repair_status(ExchangeInfo),
    ok.

%% @doc Switch over HTTP solr route and PB handling from legacy
%% riak_search to yokozuna. This will multicall to all nodes in the
%% cluster. If any nodes are down report them to stderr and return an
%% error tuple. Once the switch is made the system cannot be switched
%% back without restarting the cluster.
-spec switch_to_new_search([]) -> ok | {error, {nodes_down, [node()]}}.
switch_to_new_search([]) ->
    {_Good, Down} = riak_core_util:rpc_every_member_ann(yokozuna, switch_to_yokozuna, [], 5000),
    case Down of
        [] ->
            ok;
        _ ->
            Down2 = [atom_to_list(Node) || Node <- Down],
            DownStr = string:join(Down2, " "),
            io:format(standard_error, "The following nodes could not be reached: ~s", [DownStr]),
            {error, {nodes_down, Down}}
    end.
