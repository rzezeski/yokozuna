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

%% @doc An extractor for XML.  Convert the hierarchy to a set of
%%      fields where the field name is the element stack separated by
%%      the `field_separator' and the field value is the value of the
%%      innermost element.
%%
%% Example:
%%
%%   <person>
%%     <name>Ryan</name>
%%     <age>29</age>
%%   </person>
%%
%%   [{<<"person.name">>, <<"Ryan">>},
%%    {<<"person.age">>, <<"29">>}]
%%
%% Options:
%%
%%   `field_separator' - Use a different field separator than the default of `.'.
%%
-module(yz_xml_extractor).
-compile(export_all).
-include("yokozuna.hrl").
-define(DEFAULT_FIELD_SEPARATOR, <<".">>).
-define(DEFAULT_NS_SEPARATOR, <<"@">>).
-record(state, {
          name_stack = [],
          fields = [],
          field_separator = ?DEFAULT_FIELD_SEPARATOR,
          ns_separator = ?DEFAULT_NS_SEPARATOR
         }).

extract(Value) ->
    extract(Value, ?NO_OPTIONS).

-spec extract(binary(), proplist()) -> fields() | {error, any()}.
extract(Value, Opts) ->
    Sep = proplists:get_value(field_separator, Opts, ?DEFAULT_FIELD_SEPARATOR),
    NSSep = proplists:get_value(ns_separator, Opts, ?DEFAULT_NS_SEPARATOR),
    extract_fields(Value, #state{field_separator=Sep, ns_separator=NSSep}).

extract_fields(Data, State) ->
    Options = [{event_fun, fun sax_cb/3}, {event_state, State}],
    case xmerl_sax_parser:stream(Data, Options) of
        {ok, State2, _Rest} ->
            State2#state.fields;
        {fatal_error, Loc, Reason, _Tags, _State2} ->
            parsing_error(Loc, Reason)
    end.

sax_cb({startPrefixMapping, _Prefix, _Uri}=P, _, S) ->
    io:format("~p~n", [P]),
    S;

sax_cb({endPrefixMapping, _Prefix}=P, _, S) ->
    io:format("~p~n", [P]),
    S;

%% @private
sax_cb({startElement, NS, Name, {_NS, _Name}, Attrs}, _Location, S) ->
    Separator = S#state.field_separator,
    _NSSeparator = S#state.ns_separator,
    Name2 = case NS of
                [] ->
                    Name;
                _ ->
                    <<"{",(unicode:characters_to_binary(NS))/binary,"}",(unicode:characters_to_binary(Name))/binary>>
            end,
    io:format("startElement ~p ~p ~p ~p~n", [NS, Name, {_NS, _Name}, Attrs]),
    NewNameStack = [Name2 | S#state.name_stack],
    AttrFields = case Attrs of
                     [] ->
                         [];
                     _ ->
                         make_attr_fields(make_name(Separator, NewNameStack),
                                          Attrs, [])
                 end,
    S#state{name_stack=NewNameStack,
            fields=AttrFields ++ S#state.fields};

%% End of an element, collapse it into the previous item on the stack...
sax_cb({endElement, _Uri, _Name, _QualName}, _Location, S) ->
    io:format("endElement ~p ~p ~p~n", [_Uri, _Name, _QualName]),
    S#state{name_stack=tl(S#state.name_stack)};

%% Got a value, set it to the value of the topmost element in the stack...
sax_cb({characters, Value}, _Location, S) ->
    io:format("characters ~p~n", [Value]),
    Name = make_name(S#state.field_separator, S#state.name_stack),
    Field = {Name, unicode:characters_to_binary(Value)},
    S#state{fields = [Field|S#state.fields]};

sax_cb(_Event, _Location, State) ->
    State.

-spec make_attr_fields(binary(), list(), list()) -> [{binary(), binary()}].
make_attr_fields(_BaseName, [], Fields) ->
    Fields;
make_attr_fields(BaseName, [{_Uri, _Prefix, AttrName, Value}|Attrs], Fields) ->
    AttrNameB = unicode:characters_to_binary(AttrName),
    FieldName = <<BaseName/binary,$@,AttrNameB/binary>>,
    Field = {FieldName, unicode:characters_to_binary(Value)},
    make_attr_fields(BaseName, Attrs, [Field | Fields]).

make_name(Seperator, Stack) ->
    io:format("make_name ~p ~p~n", [Seperator, Stack]),
    make_name(Seperator, Stack, <<>>).

%% Make a name from a stack of visted tags (innermost tag at head of list)
-spec make_name(binary(), [string()], binary()) -> binary().
make_name(Seperator, [Inner,Outter|Rest], Name) ->
    OutterB = unicode:characters_to_binary(Outter),
    InnerB = unicode:characters_to_binary(Inner),
    Name2 = case Name of
                <<>> ->
                    <<OutterB/binary,Seperator/binary,InnerB/binary,Name/binary>>;
                _ ->
                    <<OutterB/binary,Seperator/binary,InnerB/binary,Seperator/binary,Name/binary>>
            end,
    make_name(Seperator, Rest, Name2);
make_name(Seperator, [Outter], Name) ->
    OutterB = unicode:characters_to_binary(Outter),
    case Name of
        <<>> -> OutterB;
        _ -> <<OutterB/binary,Seperator/binary,Name/binary>>
    end;
make_name(_, [], Name) ->
    Name.

parsing_error({_, _, Line}, Reason) ->
    Msg = io_lib:format("failure parsing XML at line ~w with reason \"~s\"",
                        [Line, Reason]),
    {error, Msg}.
