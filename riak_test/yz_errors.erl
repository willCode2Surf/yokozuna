%% @doc This is a series of "negative tests".  I.e. tests that
%%      purposely invoke errors to verify they are handled correctly.
-module(yz_errors).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-define(B, <<"yz_errors">>).
-define(BS, "yz_errors").

confirm() ->
    Cluster = prepare_cluster(4),
    ?assert(bad_json(Cluster)),
    pass.

bad_json(Cluster) ->
    %% BadJSON is missing closing " and } on purpose
    BadJSON = <<"{\"description_t\":"
                "\"Yokozuna is the combination of Riak & Solr">>,
    HP = select_random(host_entries(rt:connection_info(Cluster))),
    CT = "application/json",
    %% TODO: check for informative msg
    {ok, "400", _, _} = http_put(HP, ?B, <<"bad_json">>, BadJSON, CT).

%%%===================================================================
%%% Helpers
%%%===================================================================

create_index(Node, Index) ->
    lager:info("Creating index ~s [~p]", [Index, Node]),
    rpc:call(Node, yz_index, create, [Index]).

host_entries(ClusterConnInfo) ->
    [proplists:get_value(http, I) || {_,I} <- ClusterConnInfo].

-spec http_put({string(), integer()}, binary(), binary(), binary(), string()) ->
                      {ok, string(), Headers::list(), Body::binary()} |
                      {error, term()}.
http_put({Host, Port}, Bucket, Key, Value, CT) ->
    URL = lists:flatten(io_lib:format("http://~s:~s/riak/~s/~s",
                                      [Host, integer_to_list(Port), Bucket, Key])),
    Opts = [{response_format, binary}],
    Headers = [{"content-type", CT}],
    ibrowse:send_req(URL, Headers, put, Value, Opts).

join(Nodes) ->
    [NodeA|Others] = Nodes,
    [rt:join(Node, NodeA) || Node <- Others],
    Nodes.

prepare_cluster(NumNodes) ->
    %% Note: may need to use below call b/c of diff between
    %% deploy_nodes/1 & /2
    %%
    %% Nodes = rt:deploy_nodes(NumNodes, ?CFG),
    Nodes = rt:deploy_nodes(NumNodes),
    Cluster = join(Nodes),
    wait_for_joins(Cluster),
    setup_indexing(Cluster),
    Cluster.

select_random(List) ->
    Length = length(List),
    Idx = random:uniform(Length),
    lists:nth(Idx, List).

set_index_flag(Node, Index) ->
    lager:info("Install index hook on bucket ~s [~p]", [Index, Node]),
    rpc:call(Node, yz_kv, set_index_flag, [Index]).

setup_indexing(Cluster) ->
    Node = select_random(Cluster),
    ok = create_index(Node, ?BS),
    ok = set_index_flag(Node, ?B),
    %% Give Solr time to build index
    timer:sleep(5000).

wait_for_joins(Cluster) ->
    lager:info("Waiting for ownership handoff to finish"),
    rt:wait_until_nodes_ready(Cluster),
    rt:wait_until_no_pending_changes(Cluster).
