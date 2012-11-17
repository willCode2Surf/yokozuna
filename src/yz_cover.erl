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

-module(yz_cover).
-compile(export_all).
-include("yokozuna.hrl").

%% @doc This module contains functionality related to creating
%%      coverage information for distributed search queries.

%%%===================================================================
%%% API
%%%===================================================================

-spec logical_partitions(ring(), ordset(p())) -> ordset(lp()).
logical_partitions(Ring, Partitions) ->
    LI = logical_index(Ring),
    ordsets:from_list([logical_partition(LI, P) || P <- Partitions]).

plan(Index) ->
    case yz_events:get_cached_plan(Index) of
        {ok, Plan} -> Plan;
        none ->
            try
                gen_plan(Index)
            catch _:Reason ->
                    lager:error("failed to gen plan: ~p", [Reason]),
                    none
            end
    end.

gen_plan(Index) ->
    Ring = yz_misc:get_ring(transformed),
    Q = riak_core_ring:num_partitions(Ring),
    BProps = riak_core_bucket:get_bucket(Index, Ring),
    Selector = all,
    NVal = riak_core_bucket:n_val(BProps),
    NumPrimaries = 1,
    ReqId = erlang:phash2(erlang:now()),

    Result = riak_core_coverage_plan:create_plan(Selector,
                                                 NVal,
                                                 NumPrimaries,
                                                 ReqId,
                                                 ?YZ_SVC_NAME),
    case Result of
        {error, Error} ->
            throw(Error);
        {CoverSet, _} ->
            {_Partitions, Nodes} = lists:unzip(CoverSet),
            UniqNodes = lists:usort(Nodes),
            LPI = logical_index(Ring),
            LogicalCoverSet = add_filtering(NVal, Q, LPI, CoverSet),
            {UniqNodes, LogicalCoverSet}
    end.

-spec reify_partitions(ring(), ordset(lp())) -> ordset(p()).
reify_partitions(Ring, LPartitions) ->
    LI = logical_index(Ring),
    ordsets:from_list([partition(LI, LP) || LP <- LPartitions]).

%%%===================================================================
%%% Private
%%%===================================================================

%% @doc Create a covering set using logical partitions and add
%%      filtering information to eliminate overlap.
-spec add_filtering(n(), q(), logical_idx(), cover_set()) ->
                           [{lp_node(), logical_filter()}].
add_filtering(N, Q, LPI, CS) ->
    CS2 = make_logical(LPI, CS),
    CS3 = yz_misc:make_pairs(CS2),
    CS4 = make_distance_pairs(Q, CS3),
    make_filter_pairs(N, Q, CS4).

%% @doc Get the distance between the logical partition `LPB' and
%%      `LPA'.
-spec get_distance(q(), lp_node(), lp_node()) -> dist().
get_distance(Q, {LPA,_}, {LPB,_}) when LPB < LPA ->
    %% Wrap around
    BottomDiff = LPB - 1,
    TopDiff = Q - LPA,
    BottomDiff + TopDiff + 1;
get_distance(_Q, {LPA,_}, {LPB,_}) ->
    LPB - LPA.

%% @doc Create a mapping from logical to actual partition.
-spec logical_index(riak_core_ring:riak_core_ring()) -> logical_idx().
logical_index(Ring) ->
    {Partitions, _} = lists:unzip(riak_core_ring:all_owners(Ring)),
    Q = riak_core_ring:num_partitions(Ring),
    Logical = lists:seq(1, Q),
    lists:zip(Logical, lists:sort(Partitions)).

%% @doc Map `Partition' to it's logical partition.
-spec logical_partition(logical_idx(), p()) -> lp().
logical_partition(LogicalIndex, Partition) ->
    {Logical, _} = lists:keyfind(Partition, 2, LogicalIndex),
    Logical.

%% @doc Generate the sequence of `N' partitions leading up to `EndLP'.
%%
%% NOTE: Logical partition numbers start at 1
-spec lp_seq(n(), q(), lp()) -> [lp()].
lp_seq(N, Q, EndLP) ->
    N1 = N - 1,
    StartLP = EndLP - N1,
    if StartLP =< 0 ->
            StartLP2 = Q + StartLP,
            lists:seq(StartLP2, Q) ++ lists:seq(1, EndLP);
       true ->
            lists:seq(StartLP, EndLP)
    end.

%% @doc Take a list of `PartitionPairs' and create a list of
%%      `{LogicalPartition, Distance}' pairs.  The list will contain
%%      the second partition in the original pair and it's distance
%%      from the partition it was paired with.
-spec make_distance_pairs(q(), [{lp_node(), lp_node()}]) ->
                                 [{lp_node(), dist()}].
make_distance_pairs(Q, PartitionPairs) ->
    [{LPB, get_distance(Q, LPA, LPB)} || {LPA, LPB} <- PartitionPairs].


%% @doc Create a `{LogicalPartition, Include}' filter pair for a given
%%      `{LogicalPartition, Dist}' pair.  `Include' indicates which
%%      replicas should be included for the paired `LogicalPartition'.
%%      The value `all' means all replicas.  If the value if a list of
%%      `lp()' then a replica must has one of the LPs as it's first
%%      primary partition on the preflist.
-spec make_filter_pair(n(), q(), {lp_node(), dist()}) ->
                              {lp_node(), all | [lp()]}.
make_filter_pair(N, _Q, {LPNode, N}) ->
    {LPNode, all};
make_filter_pair(N, Q, {{LP, Node}, Dist}) ->
    LPSeq = lists:reverse(lp_seq(N, Q, LP)),
    Filter = lists:sublist(LPSeq, Dist),
    {{LP, Node}, Filter}.

-spec make_filter_pairs(n(), q(), [{lp_node(), dist()}]) ->
                               logical_cover_set().
make_filter_pairs(N, Q, Cover) ->
    [make_filter_pair(N, Q, DP) || DP <- Cover].

%% @doc Convert the `Cover' set to use logical partitions.
-spec make_logical(logical_idx(), cover_set()) -> logical_cover_set().
make_logical(LogicalIndex, Cover) ->
    [{logical_partition(LogicalIndex, P), Node} || {P, Node} <- Cover].

%% @doc Map `LP' to actual partition.
-spec partition(logical_idx(), lp()) -> p().
partition(LogicalIndex, LP) ->
    {_, P} = lists:keyfind(LP, 1, LogicalIndex),
    P.
