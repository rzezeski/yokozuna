-module(yz_solr_intercepts).
-compile(export_all).
-include("intercept.hrl").

dist_search_500(_Index, _Headers, _Params) ->
    ?I_INFO("forcing search to return 500\n"),
    {error, {"500", [], <<"dummy error injected by test intercept">>}}.
