%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc BPar - Yet another worker pool
%%%
%%% Three modes of operation
%%%  * synchronous (run)
%%%  * asynchronous (run_async)
%%%  * asynchronous, wait for a worker to become ready (run_async_wait)
%%%
%%% Start arguments, required
%%%  * mod  - worker callback module
%%%  * size - number of workers
%%%
%%% Start arguments, optional:
%%%  * queue_size - max number of enqueued tasks
%%%  * args       - arguments to callback
%%%
%%% Task arguments, optional:
%%%  * queue_timeout - timeout for task waiting in queue (default infinity)
%%%  * caller_alive  - caller must be alive for execution (default false)
%%%
%%% In addition to the queue_timeout a timeout can be placed for the
%%% entire call (defaults to 5 seconds).
%%%
%%% @copyright 2012 Klarna AB
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%
%%%   Copyright 2011-2013 Klarna AB
%%%
%%%   Licensed under the Apache License, Version 2.0 (the "License");
%%%   you may not use this file except in compliance with the License.
%%%   You may obtain a copy of the License at
%%%
%%%       http://www.apache.org/licenses/LICENSE-2.0
%%%
%%%   Unless required by applicable law or agreed to in writing, software
%%%   distributed under the License is distributed on an "AS IS" BASIS,
%%%   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%%   See the License for the specific language governing permissions and
%%%   limitations under the License.
%%%

%%%
%%%   Copyright 2013 Bjorn Jensen-Urstad

%%%_* Module declaration ===============================================
-module(bpar).
-behaviour(gen_server).

%%%_* Exports ==========================================================
%% api
-export([ start/1
        , start/2
        , start_link/1
        , start_link/2
        , stop/1
        , run/2
        , run/3
        , run/4
        , run_async/2
        , run_async/3
        , run_async/4
        , run_async_wait/2
        , run_async_wait/3
        , run_async_wait/4
        , flush/1
        , flush/2
        ]).

%% gen_server
-export([ init/1
        , terminate/2
        , code_change/3
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        ]).

%% behaviour
-export([ behaviour_info/1
        ]).

%%%_* Includes =========================================================
-include_lib("eunit/include/eunit.hrl").
-include_lib("stdlib2/include/prelude.hrl").

%%%_* Macros ===========================================================
%% defaults
-define(call_timeout, 5000).
-define(options,      [{caller_alive, false},
                       {queue_timeout, infinity}]).

-define(is_bif(Cb), (Cb =:= bpar_bif_fun)).

%%%_* Code =============================================================
%%%_ * Behaviour -------------------------------------------------------
behaviour_info(callbacks) ->
  [ {start_link, 1}
  , {stop,       1}
  , {run,        2}
  ];
behaviour_info(_) -> undefined.

%%%_ * Types -----------------------------------------------------------
-type ref()     :: atom() | pid().
-type task()    :: any().
-type options() :: list().

-record(s, { %% user supplied
             mod        = throw('mod')        :: atom()
           , args       = throw('args')       :: list()
           , size       = throw('size')       :: integer()
           , queue_size = throw('queue_size') :: integer()
             %% internal
           , free       = throw('free')       :: queue() %workers
           , busy       = gb_trees:empty()    :: list()  %workers
           , n          = 1                   :: _       %req counter
           , work       = throw('work')       :: _       %req queue
           , expire     = throw('expire')     :: _
           , flush      = []                  :: list()  %froms
           }).

-record(t, { ts      = throw('ts')      :: integer()
           , options = throw('options') :: list()
           , type    = throw('type')    :: run|run_async|run_async_wait
           , data    = throw('data')    :: any()
           , from                       :: any()
           , n                          :: undefined | integer()
         }).

%%%_ * API -------------------------------------------------------------
start(Args) ->
  gen_server:start(?MODULE, Args, []).

start(Reg, Args) ->
  gen_server:start(Reg, ?MODULE, Args, []).

start_link(Args) ->
  gen_server:start_link(?MODULE, Args, []).

start_link(Reg, Args) ->
  gen_server:start_link(Reg, ?MODULE, Args, []).

stop(Ref) ->
  gen_server:call(Ref, stop).

-spec flush(ref()) -> ok.
flush(Ref) ->
  flush(Ref, ?call_timeout).

-spec flush(ref(), timeout()) -> ok.
flush(Ref, Timeout) ->
  gen_server:call(Ref, flush, Timeout).

-spec run(ref(), task()) -> maybe(_, _).
run(Ref, Task) ->
  run(Ref, Task, []).

-spec run(ref(), task(), options()) -> maybe(_, _).
run(Ref, Task, Options) ->
  run(Ref, Task, Options, ?call_timeout).

-spec run(ref(), task(), options(), timeout()) -> maybe(_, _).
run(Ref, Task, Options, Timeout) ->
  new_task(run, Ref, Task, Options, Timeout).

-spec run_async(ref(), task()) -> whynot(_).
run_async(Ref, Task) ->
  run_async(Ref, Task, []).

-spec run_async(ref(), task(), options()) -> whynot(_).
run_async(Ref, Task, Options) ->
  run_async(Ref, Task, Options, ?call_timeout).

-spec run_async(ref(), task(), options(), timeout()) -> whynot(_).
run_async(Ref, Task, Options, Timeout) ->
  new_task(run_async, Ref, Task, Options, Timeout).

-spec run_async_wait(ref(), task()) -> maybe({ok, pid()}, _).
run_async_wait(Ref, Task) ->
  run_async_wait(Ref, Task, []).

-spec run_async_wait(ref(), task(), options()) ->
                        maybe({ok, pid()}, _).
run_async_wait(Ref, Task, Options) ->
  run_async_wait(Ref, Task, Options, ?call_timeout).

-spec run_async_wait(ref(), task(), options(), timeout()) ->
                        maybe({ok, pid()}, _).
run_async_wait(Ref, Task, Options, Timeout) ->
  new_task(run_async_wait, Ref, Task, Options, Timeout).


%%%_ * gen_server callbacks --------------------------------------------
init(Args) ->
  erlang:process_flag(trap_exit, true),
  {ok, Mod}     = s2_lists:assoc(Args, mod),
  ModArgs       = s2_lists:assoc(Args, args, []),
  {ok, Size}    = s2_lists:assoc(Args, size),
  QueueSize     = s2_lists:assoc(Args, queue_size, infinity),
  Free          = lists:foldl(
                    fun(Pid, Q) -> queue:in(Pid, Q) end, queue:new(),
                    start_workers(Mod, ModArgs, Size)),
  {ok, #s{ mod        = Mod
         , args       = ModArgs
         , size       = Size
         , queue_size = QueueSize
         , free       = Free
         , work       = ets:new(?MODULE, [ordered_set, private])
         , expire     = ets:new(?MODULE, [ordered_set, private])
         }}.

terminate(_Rsn, S) ->
  Pids = queue:to_list(S#s.free) ++ gb_trees:keys(S#s.busy),
  lists:foreach(fun(Pid) -> Pid ! {self(), stop} end, Pids),
  lists:foreach(fun(Pid) -> receive {'EXIT', Pid, _} -> ok end end, Pids).

handle_call({run, _}, _From, #s{flush=[_|_]} = S) ->
  {reply, {error, flushing}, S, wait(S#s.expire)};

handle_call({run, T0}, From, S) ->
  T = T0#t{from=From, n=S#s.n},
  case queue:out(S#s.free) of
    {{value, Pid}, Free} ->
      %% free worker
      ?hence(ets_len(S#s.work) =:= 0),
      Pid ! {self(), {run, T#t.data}},
      [gen_server:reply(From, ok)        || T#t.type =:= run_async],
      [gen_server:reply(From, {ok, Pid}) || T#t.type =:= run_async_wait],
      {noreply, S#s{ n    = S#s.n+1
                   , free = Free
                   , busy = gb_trees:insert(Pid, T, S#s.busy)
                   }};
    {empty, _Free} ->
      %% no free workers
      case ets_len(S#s.work) >= S#s.queue_size of
        true  ->
          {reply, {error, queue_full}, S, wait(S#s.expire)};
        false ->
          enq_work(T, S#s.work, S#s.expire),
          [gen_server:reply(From, ok) || T#t.type =:= run_async],
          {noreply, S#s{n=S#s.n+1}, wait(S#s.expire)}
      end
  end;

handle_call(flush, From, S) ->
  case gb_trees:size(S#s.busy) of
    0 -> ?hence(ets_len(S#s.work) =:= 0),
         ?hence(ets_len(S#s.expire) =:= 0),
         {reply, ok, S};
    _ -> {noreply, S#s{flush=[From|S#s.flush]}, wait(S#s.expire)}
  end;

handle_call(stop, _From, S) ->
  {stop, normal, ok, S}.

handle_cast(Msg, S) ->
  {stop, {bad_cast, Msg}, S}.

handle_info({Pid, {done, Res}}, S) ->
  T    = gb_trees:get(Pid, S#s.busy),
  Busy = gb_trees:delete(Pid, S#s.busy),
  [gen_server:reply(T#t.from, Res) || T#t.type =:= run],
  case deq_work(S#s.work, S#s.expire) of
    {ok, TNext} ->
      Pid ! {self(), {run, TNext#t.data}},
      [gen_server:reply(TNext#t.from, {ok, Pid}) ||
        TNext#t.type =:= run_async_wait],
      {noreply, S#s{busy=gb_trees:insert(Pid, TNext, Busy)
                   }, wait(S#s.expire)};
    {error, empty} ->
      lists:foreach(fun(From) ->
                        gen_server:reply(From, ok)
                    end, S#s.flush),
      {noreply, S#s{ busy   = Busy
                   , free   = queue:in(Pid, S#s.free)
                   , flush  = []
                   }, infinity}
  end;

handle_info(timeout, S) ->
  do_expire(s2_time:stamp() div 1000, S#s.work, S#s.expire),
  {noreply, S, wait(S#s.expire)};

handle_info({'EXIT', Pid, Rsn}, S) ->
  {Free, Busy} =
    case gb_trees:lookup(Pid, S#s.busy) of
      {value, _T} -> {S#s.free, gb_trees:delete(Pid, S#s.busy)};
      none        ->  L = queue:to_list(S#s.free),
                      ?hence(lists:member(Pid, L)),
                      {queue:from_list(L -- [Pid]), S#s.busy}
    end,
  ?error("worker died: ~p", [Rsn]),
  {stop, Rsn, S#s{free=Free, busy=Busy}};

handle_info(Msg, S) ->
  ?warning("~p", [Msg]),
  {noreply, S, wait(S#s.expire)}.

code_change(_OldVsn, S, _Extra) ->
  {ok, S, wait(S#s.expire)}.

%%%_ * Internals -------------------------------------------------------
new_task(Type, Pid, Data, Options0, Timeout) ->
  Options = lists:ukeysort(1, Options0 ++ ?options),
  case lists:all(fun is_valid_option/1, Options) of
    true ->
      gen_server:call(
        Pid, {run, #t{ ts      = s2_time:stamp() div 1000
                     , options = Options
                     , data    = Data
                     , type    = Type
                     }}, Timeout);
    false ->
      {error, bad_option}
  end.

is_valid_option({caller_alive, Bool})
  when erlang:is_boolean(Bool)             -> true;
is_valid_option({queue_timeout, infinity}) -> true;
is_valid_option({queue_timeout, N})
  when erlang:is_integer(N), N > 0         -> true;
is_valid_option(_)                         -> false.

%%%_ * Internals enqueue/dequeue  --------------------------------------
%% Two priority queues (gb_trees) are kept in sync to be able to handle
%% both queue ordering and expiration efficiently. In order to keep the
%% trees in sync the incoming requests are numbered starting from 1 (N).
%%
%% Queue has structure  {K:N, V:Task}
%% Expire has structure {K:{AbsoluteExpireTime, N}, V:N}
%%
%% Picking the next task to run or expire is then a O(log N)
%% operation.
enq_work(#t{n=N} = T, Work, Expire) ->
  case s2_lists:assoc(T#t.options, queue_timeout) of
    {ok, infinity} -> ?hence(ets:insert_new(Work, {N, T}));
    {ok, Timeout}  -> ?hence(ets:insert_new(Work, {N, T})),
                      ?hence(ets:insert_new(Expire, {{T#t.ts + Timeout, N}, N}))
  end.

deq_work(Work, Expire) ->
  ?hence(ets_len(Work) >= ets_len(Expire)),
  case ets:first(Work) of
    '$end_of_table' ->
      {error, empty};
    N ->
      [{N,#t{n=N}=T}] = ets:lookup(Work, N),
      true = ets:delete(Work, N),
      case s2_lists:assoc(T#t.options, queue_timeout) of
        {ok, infinity} -> ok;
        {ok, Timeout}  -> true = ets:delete(Expire, {T#t.ts + Timeout, T#t.n})
      end,
      {ok, CallerAlive} = s2_lists:assoc(T#t.options, caller_alive),
      {Pid, _}          = T#t.from,
      PidAlive          = erlang:is_process_alive(Pid),
      if CallerAlive, not PidAlive ->
          ?debug("caller not alive, dropping: ~p", [Task]),
          deq_work(Work, Expire);
         true ->
          {ok, T}
      end
  end.

ets_len(Tab) ->
  {ok, Len} = s2_lists:assoc(ets:info(Tab), size),
  Len.

%%%_ * Internals timeouts/expire ---------------------------------------
%% @doc ms's until next task expire
wait(Expire) ->
  case ets:first(Expire) of
    '$end_of_table' -> infinity;
    {Timeout, _N}   -> lists:max([Timeout - (s2_time:stamp() div 1000), 0])
  end.

%% @doc expire expired tasks.
do_expire(Now, Work, Expire) ->
  case ets:first(Expire) of
    '$end_of_table' -> ok;
    {Timeout, _N}
      when Timeout > Now -> ok;
    {Timeout, N} ->
      [{N,T}] = ets:lookup(Work, N),
      ets:delete(Work, N),
      ets:delete(Expire, {Timeout, N}),
      [gen_server:reply(T#t.from, {error, timeout}) ||
        T#t.type =:= run orelse T#t.type =:= run_async_wait],
      do_expire(Now, Work, Expire)
  end.

%%%_ * Internals Worker related ----------------------------------------
start_workers(Mod, Args, N) ->
  lists:map(fun(_) -> spawn_middleman(Mod, Args) end, lists:seq(1, N)).

%% Reasoning behind using a middleman process is to simplify
%% implementation of a worker, whis way the worker process doesn't need
%% to know about the pool. The worker just gets a task, executes it
%% and returns.
spawn_middleman(Mod, Args) ->
  Daddy = self(),
  erlang:spawn_link(fun() -> middleman(Mod, Args, Daddy) end).

middleman(Mod, _Args, Daddy)
  when ?is_bif(Mod) ->
  middleman_loop(Mod, undefined, Daddy);
middleman(Mod, Args, Daddy) ->
  {ok, Pid} = Mod:start_link(Args),
  middleman_loop(Mod, Pid, Daddy).

middleman_loop(Mod, Pid, Daddy) ->
  receive
    {Daddy, {run, Data}} ->
      Daddy ! {self(), {done, run_task(Mod, Pid, Data)}},
      middleman_loop(Mod, Pid, Daddy);
    {Daddy, stop} when ?is_bif(Mod) -> ok;
    {Daddy, stop}                   -> Mod:stop(Pid);
    Msg ->
      ?warning("~p", [Msg]),
      middleman_loop(Mod, Pid, Daddy)
  end.

run_task(Mod, undefined, Task)
  when ?is_bif(Mod)      -> run_pool_bif(Mod, Task);
run_task(Mod, Pid, Task) -> Mod:run(Pid, Task).

run_pool_bif(bpar_bif_fun, Fun) -> Fun().

%%%_* Tests ============================================================
-ifdef(TEST).

start_stop_test() ->
  {ok, Pid1} = bpar:start(test_worker(8)),
  {ok, Pid2} = bpar:start({local, bpar2}, test_worker(8)),
  {ok, Pid3} = bpar:start_link(test_worker(8)),
  {ok, Pid4} = bpar:start_link({local, bpar4}, test_worker(8)),
  Pid2 = whereis(bpar2),
  Pid4 = whereis(bpar4),
  ok = bpar:stop(Pid1),
  ok = bpar:stop(Pid2),
  ok = bpar:stop(Pid3),
  ok = bpar:stop(Pid4),
  ok.

return_values_test() ->
  {ok, Pid} = bpar:start_link(test_worker(2)),
  %% run
  {ok, foo} = bpar:run(Pid, {execute, fun() -> {ok, foo} end}),
  {ok, bar} = bpar:run(Pid, {execute, fun() -> {ok, bar} end}),
  %% run_async
  ok = bpar:run_async(Pid, {execute, fun() -> foo end}),
  ok = bpar:run_async(Pid, {execute, fun() -> bar end}),
  %% run_async_wait
  {ok, WPid1} = bpar:run_async_wait(Pid, {execute, fun() -> baz end}),
  {ok, WPid2} = bpar:run_async_wait(Pid, {execute, fun() -> buz end}),
  true = erlang:is_pid(WPid1),
  true = erlang:is_pid(WPid2),
  ok = bpar:stop(Pid),
  ok.

options_test() ->
  F = fun(Options) -> bpar:run(dummy, dummy, Options) end,
  {error, bad_option} = F([{queue_timeout, 0}]),
  {error, bad_option} = F([{queue_timeout, -1}]),
  {error, bad_option} = F([{caller_alive, bar}]),
  {error, bad_option} = F([{unknown_option, x}]),
  ok.

options_queue_timeout_test() ->
  {ok, Pid} = bpar:start_link(test_worker(1)),
  Daddy     = self(),
  Task      = fun(Id) -> {execute, fun() -> Daddy ! Id end} end,

  %% make worker busy
  ok = bpar:run_async(Pid, {execute, fun() -> timer:sleep(1000) end}),

  %% call fails but task executed
  {'EXIT', {timeout, _}} =
    (catch bpar:run(Pid, Task({1, success}), [], 0)),

  %% queue timeout
  {error, timeout} = bpar:run(
                       Pid, Task({2, fail}), [{queue_timeout, 50}]),
  ok = bpar:run_async(Pid, Task({3, fail}), [{queue_timeout, 100}]),
  ok = bpar:run_async(Pid, Task({4, fail}), [{queue_timeout, 200}]),

  %% no queue timeout
  ok = bpar:run_async(Pid, Task({5, success}), [{queue_timeout, 5000}]),
  ok = bpar:flush(Pid),

  {messages, Messages} = erlang:process_info(self(), messages),
  true = lists:all(fun({_N, fail}) -> false;
                      (_         ) -> true
                   end, Messages),
  true = lists:member({1, success}, Messages),
  true = lists:member({5, success}, Messages),
  bpar:stop(Pid),
  ok.

options_caller_alive_test() ->
  {ok, Pid} = bpar:start_link([{mod, bpar_bif_fun}, {size, 1}]),
  %% make worker busy
  bpar:run_async(Pid, fun() -> timer:sleep(1000) end),
  Daddy = self(),
  F = fun() ->
          bpar:run_async(Pid, fun() -> Daddy ! foo end,
                         [{caller_alive, true}]),
          bpar:run_async(Pid, fun() -> Daddy ! bar end,
                         [{caller_alive, false}])
      end,
  proc_lib:spawn_link(F),
  receive foo -> erlang:error(caller_alive);
          bar -> ok
  end,
  bpar:stop(Pid),
  ok.

flush_test() ->
  {ok, Pid}  = bpar:start_link(test_worker(2)),
  Task       = {execute, fun() -> timer:sleep(1000) end},
  ok         = bpar:flush(Pid),
  ok         = bpar:run_async(Pid, Task),
  Pid2 = erlang:spawn_link(fun() -> bpar:flush(Pid, 5000) end),
  until_process_info(Pid2, {status, waiting}),
  {error, flushing} = bpar:run_async(Pid, Task),
  {'EXIT', {timeout, _}} = (catch bpar:flush(Pid, 500)),
  ok   = bpar:flush(Pid, 5000),
  ok   = bpar:stop(Pid),
  ok.

queue_full_test() ->
  Args      = [{mod, bpar_bif_fun},
               {size,  1},
               {queue_size, 2}],
  {ok, Pid} = bpar:start_link(Args),
  F         = fun() -> timer:sleep(1000) end,

  ok = bpar:run_async(Pid, F),   %% running
  ok = bpar:run_async(Pid, F),   %% in queue
  ok = bpar:run_async(Pid, F),   %% in queue
  {error, queue_full} = bpar:run(Pid, F),
  {error, queue_full} = bpar:run_async(Pid, F),
  {error, queue_full} = bpar:run_async_wait(Pid, F),
  bpar:stop(Pid),
  ok.

stray_messages_test() ->
  {ok, Pid} = bpar:start_link(test_worker(8)),
  Pid ! oops,
  {links, Middlemen} = erlang:process_info(Pid, links),
  lists:foreach(fun(MM) -> MM ! oops end, Middlemen),
  ok = bpar:run(Pid, {execute, fun() -> ok end}),
  bpar:stop(Pid),
  ok.

bad_cast_test() ->
  {ok, Pid} = bpar:start([{mod, bpar_test_worker}, {args, []}, {size, 8}]),
  gen_server:cast(Pid, meh),
  until_dead(Pid),
  ok.

busy_worker_crash_test() ->
  {ok, Pid}   = bpar:start(test_worker(8)),
  {ok, _WPid} = bpar:run_async_wait(Pid, {execute, fun() -> exit(die) end}),
  until_dead(Pid),
  ok.

free_worker_crash_test() ->
  {ok, Pid}  = bpar:start(test_worker(8)),
  {ok, WPid} = bpar:run(Pid, {execute, fun() -> {ok, self()} end}),
  exit(WPid, die),
  until_dead(Pid),
  ok.

code_change_test() ->
  Args = [{mod, bpar_bif_fun}, {size, 8}],
  {ok, Pid} = bpar:start_link({local, bpar}, Args),
  sys:suspend(Pid, infinity),
  sys:change_code(bpar, bpar, old_vsn, extra),
  sys:resume(Pid, infinity),
  bpar:stop(Pid),
  ok.

full_cover_test() ->
  _         = bpar:behaviour_info(callbacks),
  undefined = bpar:behaviour_info(blah),
  ok.

test_worker(Size) ->
  [{mod, bpar_test_worker}, {args, []}, {size, Size}].

until_process_info(Pid, {K, V}) ->
  case erlang:process_info(Pid, K) of
    {K, V} -> ok;
    {K, _} -> until_process_info(Pid, {K, V})
  end.

until_dead(Pid) ->
  case erlang:is_process_alive(Pid) of
    true  -> until_dead(Pid);
    false -> ok
  end.

-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:

