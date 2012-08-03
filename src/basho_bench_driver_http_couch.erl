%% -------------------------------------------------------------------
%%
%% basho_bench: Benchmarking Suite
%%
%% Copyright (c) 2012 Alexey Zatvornitskiy, <a.zatvornitskiy@gmail.com>
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
-module(basho_bench_driver_http_couch).

-export([new/1,
          run/4]).

-include("basho_bench.hrl").


-record(url, {abspath, host, port, username, password, path, protocol, host_type}).

-record(state, { base_urls,           % Tuple of #url -- one for each IP
                 base_urls_index,
                 path_params = "",
                 json_fixture }).  % #url to use for next request


%% ====================================================================
%% API
%% ====================================================================
new(_Id) ->

  %% Make sure ibrowse is available
  case code:which(ibrowse) of
      non_existing ->
          ?FAIL_MSG("~s requires ibrowse to be installed.\n", [?MODULE]);
      _ ->
        application:start(ibrowse),
        ok
  end,

  CouchConfig = basho_bench_config:get(couch_config, []),
  [ok = application:set_env(couch_config, K, V) || {K, V} <- CouchConfig],

  %% The IPs, port and path we'll be testing
  Ips  = basho_bench_config:get(couch_config, ips, ["127.0.0.1"]),
  Port = basho_bench_config:get(couch_config, port, 5984),
  Path = basho_bench_config:get(couch_config, database, "/basho_bench"),
  Abspath = basho_bench_config:get(couch_config, design_doc, ""),

  %% If there are multiple URLs, convert the list to a tuple so we can efficiently
  %% round-robin through them.
  case length(Ips) of
      1 ->
          [Ip] = Ips,
          BaseUrls = #url { host = Ip, port = Port, path = Path, abspath = Abspath },
          BaseUrlsIndex = 1;
      _ ->
          BaseUrls = list_to_tuple([ #url { host = Ip, port = Port, path = Path, abspath = Abspath } || Ip <- Ips]),
          BaseUrlsIndex = random:uniform(tuple_size(BaseUrls))
  end,

  FixtureFile = basho_bench_config:get(couch_config, json_template_path, unknown),
  case FixtureFile of
    unknown ->
        JsonFixture = "",
        ?INFO("~s couldn't find json_template_path in ~s\n", [?MODULE, FixtureFile]);

    _ ->
        {ok, Json} = file:read_file("../../" ++ FixtureFile),

        DecodedJson = try
            json_decode(Json)
        catch _:_ ->
            ?FAIL_MSG("~s Invalid JSON in ~s\n", [?MODULE, FixtureFile])
        end,

        % % eliminate white spaces
        JsonFixtureTmp = {[
            {docs, lists:duplicate(basho_bench_config:get(couch_config, batch_size, 100), DecodedJson)}
        ]},
        JsonFixture = iolist_to_binary(json_encode(JsonFixtureTmp))
  end,

  % result
  {ok, #state { base_urls = BaseUrls, base_urls_index = BaseUrlsIndex, json_fixture = JsonFixture }}.



run(get, KeyGen, _V, State) ->
  {NextUrl, S2} = next_url(State),
  run_get(S2, NextUrl#url { path = lists:concat([NextUrl#url.path, '/', KeyGen()]) });

run(get_design, KeyGen, _V, State) ->
  {NextUrl, S2} = next_url(State),
  run_get(S2, NextUrl#url { path = lists:concat([NextUrl#url.path, '/', NextUrl#url.abspath, '/', KeyGen()]) });

run(post, _K, _V, State) ->
  {NextUrl, S2} = next_url(State),
  run_submit(post, S2, NextUrl#url { path = lists:concat([NextUrl#url.path, '/']) }, State#state.json_fixture);

run(put, KeyGen, _V, State) ->
  {NextUrl, S2} = next_url(State),
  run_submit(put, S2, NextUrl#url { path = lists:concat([NextUrl#url.path, '/', KeyGen()]) }, State#state.json_fixture).


run_get(S2, Path) ->
  case do_get(Path) of
      {not_found, _Url} ->
          {ok, S2};
      {ok, _Url, _Headers} ->
          {ok, S2};
      {error, Reason} ->
          {error, Reason, S2}
  end.


run_submit(Method, S2, Path, Body) ->
  case do_submit(Method, Path, [], Body) of
      ok ->
          {ok, S2};
      {error, Reason} ->
          {error, Reason, S2}
  end.



do_get(Url) ->
  case send_request(Url, [], get, [], []) of
      {ok, "404", _Headers, _Body} ->
          {not_found, Url};
      {ok, "300", Headers, _Body} ->
          {ok, Url, Headers};
      {ok, "200", Headers, _Body} ->
          {ok, Url, Headers};
      {ok, Code, _Headers, _Body} ->
          {error, {http_error, Code}};
      {error, Reason} ->
          {error, Reason}
  end.


do_submit(Method, Url, Headers, Body) ->
    case send_request(Url, Headers ++ [{'Content-Type', 'application/json'}], Method, Body, []) of
        {ok, "201", _Header, _Body} ->
            ok;
        {ok, "204", _Header, _Body} ->
            ok;
        {ok, Code, _Header, _Body} ->
            {error, {http_error, Code}};
        {error, Reason} ->
            {error, Reason}
    end.



next_url(State) when is_record(State#state.base_urls, url) ->
    {State#state.base_urls, State};
next_url(State) when State#state.base_urls_index > tuple_size(State#state.base_urls) ->
    { element(1, State#state.base_urls),
      State#state { base_urls_index = 1 } };
next_url(State) ->
    { element(State#state.base_urls_index, State#state.base_urls),
      State#state { base_urls_index = State#state.base_urls_index + 1 }}.



connect(Url) ->
    case erlang:get({ibrowse_pid, Url#url.host}) of
        undefined ->
            {ok, Pid} = ibrowse_http_client:start({Url#url.host, Url#url.port}),
            erlang:put({ibrowse_pid, Url#url.host}, Pid),
            Pid;
        Pid ->
            case is_process_alive(Pid) of
                true ->
                    Pid;
                false ->
                    erlang:erase({ibrowse_pid, Url#url.host}),
                    connect(Url)
            end
    end.


disconnect(Url) ->
    case erlang:get({ibrowse_pid, Url#url.host}) of
        undefined ->
            ok;
        OldPid ->
            catch(ibrowse_http_client:stop(OldPid))
    end,
    erlang:erase({ibrowse_pid, Url#url.host}),
    ok.

maybe_disconnect(Url) ->
    case erlang:get(disconnect_freq) of
        infinity -> ok;
        {ops, Count} -> should_disconnect_ops(Count,Url) andalso disconnect(Url);
        Seconds -> should_disconnect_secs(Seconds,Url) andalso disconnect(Url)
    end.

should_disconnect_ops(Count, Url) ->
    Key = {ops_since_disconnect, Url#url.host},
    case erlang:get(Key) of
        undefined ->
            erlang:put(Key, 1),
            false;
        Count ->
            erlang:put(Key, 0),
            true;
        Incr ->
            erlang:put(Key, Incr + 1),
            false
    end.

should_disconnect_secs(Seconds, Url) ->
    Key = {last_disconnect, Url#url.host},
    case erlang:get(Key) of
        undefined ->
            erlang:put(Key, erlang:now()),
            false;
        Time when is_tuple(Time) andalso size(Time) == 3 ->
            Diff = timer:now_diff(erlang:now(), Time),
            if
                Diff >= Seconds * 1000000 ->
                    erlang:put(Key, erlang:now()),
                    true;
                true -> false
            end
    end.

clear_disconnect_freq(Url) ->
    case erlang:get(disconnect_freq) of
        infinity -> ok;
        {ops, _Count} -> erlang:put({ops_since_disconnect, Url#url.host}, 0);
        _Seconds -> erlang:put({last_disconnect, Url#url.host}, erlang:now())
    end.

send_request(Url, Headers, Method, Body, Options) ->
    send_request(Url, Headers, Method, Body, Options, 3).

send_request(_Url, _Headers, _Method, _Body, _Options, 0) ->
    {error, max_retries};
send_request(Url, Headers, Method, Body, Options, Count) ->
    Timeout = basho_bench_config:get(couch_config, request_timeout, 5000),

    case catch(ibrowse_http_client:send_req(connect(Url), Url, Headers, Method, Body, Options, Timeout)) of
        {ok, Status, RespHeaders, RespBody} ->
            maybe_disconnect(Url),
            {ok, Status, RespHeaders, RespBody};

        Error ->
            clear_disconnect_freq(Url),
            disconnect(Url),
            case should_retry(Error) of
                true ->
                    send_request(Url, Headers, Method, Body, Options, Count-1);

                false ->
                    normalize_error(Method, Error)
            end
    end.


should_retry({error, send_failed})       -> true;
should_retry({error, connection_closed}) -> true;
should_retry({'EXIT', {normal, _}})      -> true;
should_retry({'EXIT', {noproc, _}})      -> true;
should_retry(_)                          -> false.

normalize_error(Method, {'EXIT', {timeout, _}})  -> {error, {Method, timeout}};
normalize_error(Method, {'EXIT', Reason})        -> {error, {Method, 'EXIT', Reason}};
normalize_error(Method, {error, Reason})         -> {error, {Method, Reason}}.




json_decode(V) ->
  try (mochijson2:decoder([{object_hook, fun({struct, L}) -> {L} end}]))(V)
  catch
      _Type:_Error ->
          throw({invalid_json, V})
  end.

json_encode(V) ->
  Handler =
  fun({L}) when is_list(L) ->
      {struct, L};
  (Bad) ->
      exit({json_encode, {bad_term, Bad}})
  end,

  (mochijson2:encoder([{handler, Handler}]))(V).


