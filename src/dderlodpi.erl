-module(dderlodpi).
-behaviour(gen_server).

-include("dderlodpi.hrl").

%% API
-export([
    exec/3,                 %   ✓
    exec/4,                 %   ✓
    change_password/4,      %   ✓
    add_fsm/2,              %
    fetch_recs_async/3,     %
    fetch_close/1,          %
    filter_and_sort/6,      %
    close/1,                %
    close_port/1,           %
    run_table_cmd/3,        %   ✓
    cols_to_rec/2,          %
    get_alias/1,            %
    fix_row_format/4,       %
    create_rowfun/3         %
]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-record(qry, {select_sections
             ,contain_rowid
             ,stmt_result
             ,fsm_ref
             ,max_rowcount
             ,pushlock
             ,contain_rownum
             ,connection
             }).

-define(PREFETCH_SIZE, 250).

%% ===================================================================
%% Exported functions
%% ===================================================================
-spec exec(map(), binary(), integer()) -> ok | {ok, pid()} | {error, term()}.
exec(Connection, Sql, MaxRowCount) ->
    exec(Connection, Sql, undefined, MaxRowCount).

-spec exec(map(), binary(), tuple(), integer()) -> ok | {ok, pid()} | {error, term()}.
exec(Connection, OrigSql, Binds, MaxRowCount) ->
    {Sql, NewSql, TableName, RowIdAdded, SelectSections} =
        parse_sql(sqlparse:parsetree(OrigSql), OrigSql),
    case catch run_query(Connection, Sql, Binds, NewSql, RowIdAdded, SelectSections) of
        {'EXIT', {{error, Error}, ST}} ->
            ?Error("run_query(~s,~p,~s)~n{~p,~p}", [Sql, Binds, NewSql, Error, ST]),
            {error, Error};
        {'EXIT', {Error, ST}} ->
            ?Error("run_query(~s,~p,~s)~n{~p,~p}", [Sql, Binds, NewSql, Error, ST]),
            {error, Error};
        {ok, #stmtResult{} = StmtResult, ContainRowId} ->
            LowerSql = string:to_lower(binary_to_list(Sql)),
            case string:str(LowerSql, "rownum") of
                0 -> ContainRowNum = false;
                _ -> ContainRowNum = true
            end,
            {ok, Pid} = gen_server:start(?MODULE, [
                SelectSections, StmtResult, ContainRowId, MaxRowCount, ContainRowNum, Connection
            ], []),
            SortSpec = gen_server:call(Pid, build_sort_spec, ?ExecTimeout),
            %% Mask the internal stmt ref with our pid.
            {ok, StmtResult#stmtResult{stmtRef = Pid, sortSpec = SortSpec}, TableName};
        NoSelect ->
            NoSelect
    end.

-spec append_semicolon(binary(), integer()) -> binary().
append_semicolon(Sql, $;) -> Sql;
append_semicolon(Sql, _) -> <<Sql/binary, $;>>.

-spec change_password(tuple(), binary(), binary(), binary()) -> ok | {error, term()}.
change_password({oci_port, _, _} = Connection, User, OldPassword, NewPassword) ->
    run_table_cmd(Connection, iolist_to_binary(["ALTER USER ", User, " IDENTIFIED BY ", NewPassword, " REPLACE ", OldPassword])).

-spec add_fsm(pid(), term()) -> ok.
add_fsm(Pid, FsmRef) ->
    gen_server:cast(Pid, {add_fsm, FsmRef}).

-spec fetch_recs_async(pid(), list(), integer()) -> ok.
fetch_recs_async(Pid, Opts, Count) ->
    gen_server:cast(Pid, {fetch_recs_async, lists:member({fetch_mode, push}, Opts), Count}).

-spec fetch_close(pid()) -> ok.
fetch_close(Pid) ->
    gen_server:call(Pid, fetch_close, ?ExecTimeout).

-spec filter_and_sort(pid(), tuple(), list(), list(), list(), binary()) -> {ok, binary(), fun()}.
filter_and_sort(Pid, Connection, FilterSpec, SortSpec, Cols, Query) ->
    gen_server:call(Pid, {filter_and_sort, Connection, FilterSpec, SortSpec, Cols, Query}, ?ExecTimeout).

-spec close(pid()) -> term().
close(Pid) ->
    gen_server:call(Pid, close, ?ExecTimeout).

-spec close_port(tuple()) -> term().
close_port({OciMod, PortPid, _Conn}) -> close_port({OciMod, PortPid});
close_port({_OciMod, _PortPid} = Port) -> oci_port:close(Port).

%% Gen server callbacks
init([SelectSections, StmtResult, ContainRowId, MaxRowCount, ContainRowNum, Connection]) ->
    {ok, #qry{
            select_sections = SelectSections,
            stmt_result = StmtResult,
            contain_rowid = ContainRowId,
            max_rowcount = MaxRowCount,
            contain_rownum = ContainRowNum,
            connection = Connection}}.

handle_call({filter_and_sort, Connection, FilterSpec, SortSpec, Cols, Query}, _From, #qry{stmt_result = StmtResult} = State) ->
    #stmtResult{stmtCols = StmtCols} = StmtResult,
    %% TODO: improve this to use/update parse tree from the state.
    Res = filter_and_sort_internal(Connection, FilterSpec, SortSpec, Cols, Query, StmtCols),
    {reply, Res, State};
handle_call(build_sort_spec, _From, #qry{stmt_result = StmtResult, select_sections = SelectSections} = State) ->
    #stmtResult{stmtCols = StmtCols} = StmtResult,
    SortSpec = build_sort_spec(SelectSections, StmtCols),
    {reply, SortSpec, State};
handle_call(get_state, _From, State) ->
    {reply, State, State};
handle_call(fetch_close, _From, #qry{} = State) ->
    {reply, ok, State#qry{pushlock = true}};
handle_call(close, _From, #qry{connection = Connection, stmt_result = StmtResult} = State) ->
    #stmtResult{stmtRef = StmtRef} = StmtResult,
    dpi_stmt_close(Connection, StmtRef),
    {stop, normal, ok, State#qry{stmt_result = StmtResult#stmtResult{stmtRef = undefined}}};
handle_call(_Ignored, _From, State) ->
    {noreply, State}.

handle_cast({add_fsm, FsmRef}, #qry{} = State) -> {noreply, State#qry{fsm_ref = FsmRef}};
handle_cast({fetch_recs_async, _, _}, #qry{pushlock = true} = State) ->
    {noreply, State};
handle_cast({fetch_push, _, _}, #qry{pushlock = true} = State) ->
    {noreply, State};
handle_cast({fetch_recs_async, true, FsmNRows}, #qry{max_rowcount = MaxRowCount} = State) ->
    case FsmNRows rem MaxRowCount of
        0 -> RowsToRequest = MaxRowCount;
        Result -> RowsToRequest = MaxRowCount - Result
    end,
    gen_server:cast(self(), {fetch_push, 0, RowsToRequest}),
    {noreply, State};
handle_cast({fetch_recs_async, false, _}, #qry{fsm_ref = FsmRef, stmt_result = StmtResult,
        contain_rowid = ContainRowId, connection = Connection} = State) ->
    #stmtResult{stmtRef = Statement, stmtCols = Clms} = StmtResult,
    Res = dpi_fetch_rows(Connection, Statement, ?DEFAULT_ROW_SIZE),
    ?Info("fetch rows result ~p", [Res]),
    case Res of
        {error, Error} -> FsmRef:rows({error, Error});
        {error, _DpiNifFile, _Line, #{message := Msg}} -> FsmRef:rows({error, Msg});
        {Rows, Completed} when is_list(Rows), is_boolean(Completed) ->
            try FsmRef:rows({fix_row_format(Statement, Rows, Clms, ContainRowId), Completed}) of
                ok -> ok
            catch
                _Class:Result ->
                    FsmRef:rows({error, Result})
            end
    end,
    {noreply, State};
handle_cast({fetch_push, NRows, Target}, #qry{fsm_ref = FsmRef, stmt_result = StmtResult} = State) ->
    #qry{contain_rowid = ContainRowId, contain_rownum = ContainRowNum} = State,
    #stmtResult{stmtRef = StmtRef, stmtCols = Clms} = StmtResult,
    MissingRows = Target - NRows,
    if
        MissingRows > ?DEFAULT_ROW_SIZE ->
            RowsToFetch = ?DEFAULT_ROW_SIZE;
        true ->
            RowsToFetch = MissingRows
    end,
    case StmtRef:fetch_rows(RowsToFetch) of
        {{rows, Rows}, Completed} ->
            RowsFixed = fix_row_format(StmtRef, Rows, Clms, ContainRowId),
            NewNRows = NRows + length(RowsFixed),
            if
                Completed -> FsmRef:rows({RowsFixed, Completed});
                (NewNRows >= Target) andalso (not ContainRowNum) -> FsmRef:rows_limit(NewNRows, RowsFixed);
                true ->
                    FsmRef:rows({RowsFixed, false}),
                    gen_server:cast(self(), {fetch_push, NewNRows, Target})
            end;
        {error, Error} ->
            FsmRef:rows({error, Error})
    end,
    {noreply, State};
handle_cast(_Ignored, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #qry{stmt_result = #stmtResult{stmtRef = undefined}}) -> ok;
terminate(_Reason, #qry{connection = Connection, stmt_result = #stmtResult{stmtRef = Stmt}}) ->
    dpi_stmt_close(Connection, Stmt).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Internal functions %%%
-spec select_type(list()) -> atom().
select_type(Args) ->
    Opts = proplists:get_value(opt, Args, <<>>),
    GroupBy = proplists:get_value('group by', Args),
    NotAgregation = case proplists:get_value(from, Args) of
        [] -> false;
        FromTargets -> not is_agregation(FromTargets)
    end,
    if Opts =:= <<>> andalso
       GroupBy =:= [] andalso
       NotAgregation -> select;
       true -> agregation
    end.

-spec is_agregation([binary() | tuple()]) -> boolean().
is_agregation([]) -> false;
is_agregation([Table | Rest]) when is_binary(Table) ->
    is_agregation(Rest);
is_agregation([{as, Table, Alias} | Rest]) when is_binary(Alias), is_binary(Table) ->
    is_agregation(Rest);
is_agregation(_) -> true.

-spec inject_rowid(atom(), list(), binary()) -> {binary(), binary(), boolean()}.
inject_rowid(agregation, Args, Sql) ->
    {from, [FirstTable|_]=_Forms} = lists:keyfind(from, 1, Args),
    %% Do not add rowid on agregation.
    {FirstTable, Sql, false};
inject_rowid(select, Args, Sql) ->
    {fields, Flds} = lists:keyfind(fields, 1, Args),
    {from, [FirstTable|_]=Forms} = lists:keyfind(from, 1, Args),
    NewFields = expand_star(Flds, Forms) ++ [add_rowid_field(FirstTable)],
    NewArgs = lists:keyreplace(fields, 1, Args, {fields, NewFields}),
    NPT = {select, NewArgs},
    case sqlparse_fold:top_down(sqlparse_format_flat, NPT, []) of
        {error, _Reason} ->
            {FirstTable, Sql, false};
        NewSql ->
            {FirstTable, NewSql, true}
    end.

-spec add_rowid_field(tuple() | binary()) -> binary().
add_rowid_field(Table) -> qualify_field(Table, "ROWID").

-spec qualify_field(tuple() | binary(), binary() | list()) -> binary().
qualify_field(Table, Field) -> iolist_to_binary(add_field(Table, Field)).

-spec add_field(tuple() | binary(), binary() | list()) -> iolist().
add_field({as, _, Alias}, Field) -> [Alias, ".", Field];
add_field({{as, _, Alias}, _}, Field) -> [Alias, ".", Field];
add_field({Tab, _}, Field) when is_binary(Tab) ->
    include_at(binary:split(Tab, <<"@">>), Field);
add_field(Tab, Field) when is_binary(Tab) ->
    include_at(binary:split(Tab, <<"@">>), Field).

-spec include_at(list(), binary() | list()) -> iolist().
include_at([TabName, TabLocation], Field) ->
    [TabName, ".", Field, $@, TabLocation];
include_at([TabName], Field) ->
    [TabName, ".", Field].

-spec expand_star(list(), list()) -> list().
expand_star([<<"*">>], Forms) -> qualify_star(Forms);
expand_star(Flds, _Forms) -> Flds.

-spec qualify_star(list()) -> list().
qualify_star([]) -> [];
qualify_star([Table | Rest]) -> [qualify_field(Table, "*") | qualify_star(Rest)].

bind_exec_stmt(Conn, Stmt, undefined) ->
    Res = dpi_stmt_execute(Conn, Stmt),
    ?Info("bind exec pass undefined result ~p", [Res]),
    Res;
bind_exec_stmt(Conn, Stmt, {BindsMeta, BindVal}) ->
    io:format("bind exec pass ~p~n",[0]),
    BindVars = bind_vars(Conn, Stmt, BindsMeta),
    io:format("bind exec pass ~p~n",[1]),
    execute_with_binds(Stmt, BindVars, BindVal),
    io:format("bind exec pass ~p~n",[2]),
    [ dpi:var_release(maps:get(var, B))|| B <- BindVars],
    io:format("bind exec pass ~p~n",[3]),
ok.

oraTypeToInternal(OraType)->
    case OraType of
        'SQLT_INT' -> { 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64' };
        'SQLT_CHR' -> { 'DPI_ORACLE_TYPE_CHAR', 'DPI_NATIVE_TYPE_BYTES' };
        'SQLT_FLT' -> { 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE' };
        'SQLT_DAT' -> { 'DPI_ORACLE_TYPE_DATE', 'DPI_NATIVE_TYPE_TIMESTAMP' };
        Else ->       { error, {"Unknown Type", Else}}
    end.


bind_vars(Conn, Stmt, BindsMeta)->
    
    [begin
        {OraNative, DpiNative} = oraTypeToInternal(BindType),
        Var = dpi:conn_newVar(Conn, OraNative, DpiNative, 100, 4000, false, false, undefined),
        ok = dpi:stmt_bindByName(Stmt, BindName, Var),
        {Var, DpiNative}
    end || {BindName, BindType} <- BindsMeta].

    % BindsMeta is a list like:
    %[
    %    {<<":pkey">>, 'SQLT_INT'}
    %    , {<<":publisher">>, 'SQLT_CHR'}
    %    , {<<":rank">>, 'SQLT_FLT'}
    %    , {<<":hero">>, 'SQLT_CHR'}
    %    , {<<":reality">>, 'SQLT_CHR'}
    %    , {<<":votes">>, 'SQLT_INT'}
    %    , {<<":createdate">>, 'SQLT_DAT'}
    %    , {<<":votes_first_rank">>, 'SQLT_INT'}
    %]

    % For each of those:
    %   - Create the dpiData/Var
    %   - do the bind


execute_with_binds(Stmt, BindVars, Binds) ->

    [
        [
                case VarType of 
                    'DPI_NATIVE_TYPE_INT64' ->
                         dpi:data_setInt64(Data, Bind);
                    'DPI_NATIVE_TYPE_DOUBLE' ->
                         dpi:data_setDouble(Data, Bind);
                    'DPI_NATIVE_TYPE_BYTES' ->
                         dpi:var_setFromBytes(Var, 0, Bind);
                        %% TODO: timestamp, etc

                    Else -> {error, {"invalide gobshite", Else}}
                end

        || {Bind, {#{var := Var, data := Data}, VarType}} <- lists:zip(tuple_to_list(BindTuple), BindVars)
        ]
    || BindTuple <- Binds],

    Cols = dpi:stmt_execute(Stmt, []),
    {cols,Cols}.
    % Binds is a list like 

    %[
    %   {1,<<"_publisher_1_">>,1.5,<<"_hero_1_">>,<<"_reality_1_">>, 1, <<120,119,4,10,14,31,48>>, 1},
    %   {2,<<"_publisher_2_">>,3.0,<<"_hero_2_">>,<<"_reality_2_">>, 2, <<120,119,4,10,14,31,48>>, 2},
    %   {3,<<"_publisher_3_">>,4.5,<<"_hero_3_">>,<<"_reality_3_">>, 3, <<120,119,4,10,14,31,48>>, 3}
    %]

    % For each of those:
    %   - Assign all these values to the data/char
    %   - execute the statement

run_query(Connection, Sql, Binds, NewSql, RowIdAdded, SelectSections) ->
    %% For now only the first table is counted.
    case dpi_conn_prepareStmt(Connection, NewSql) of
        Statement when is_reference(Statement) ->
            ?Info("The statement ~p", [Statement]),
            StmtExecResult = bind_exec_stmt(Connection, Statement, Binds),
            ?Info("The statement exec result ~p", [StmtExecResult]),
            case dpi_stmt_getInfo(Connection, Statement) of
                #{isQuery := true} ->
                    result_exec_query(
                        StmtExecResult,
                        Statement,
                        Sql,
                        Binds,
                        NewSql,
                        RowIdAdded,
                        Connection,
                        SelectSections
                    );
                GetInfo ->
                    ?Info("Result get_info stmt ~p", [GetInfo]),
                    result_exec_stmt(StmtExecResult,Statement,Sql,Binds,NewSql,RowIdAdded,Connection,
                             SelectSections)
            end;
        {error, _DpiNifFile, _Line, #{message := Msg}} -> error(list_to_binary(Msg));
        Error -> error(Error)
    end.

result_exec_query(NColumns, Statement, _Sql, _Binds, NewSql, RowIdAdded, Connection,
                    SelectSections) when is_integer(NColumns), NColumns > 0 ->
    ?Info("result_exec_stmt col count ~p", [NColumns]),
    ?Info("RowIdAdded: ~p", [RowIdAdded]),
    Clms = dpi_query_columns(Connection, Statement, NColumns),
    ?Info("Clms: ~p", [Clms]),
    if
        RowIdAdded -> % ROWID is hidden from columns
            [_|ColumnsR] = lists:reverse(Clms),
            Columns = lists:reverse(ColumnsR);
        true ->
            Columns = Clms
    end,
    Fields = proplists:get_value(fields, SelectSections, []),
    NewClms = cols_to_rec(Columns, Fields),
    SortFun = build_sort_fun(NewSql, NewClms),
    {ok
     , #stmtResult{ stmtCols = NewClms
                    , rowFun   =
                        fun({{}, Row}) ->
                                if
                                    RowIdAdded ->
                                        [_|NewRowR] = lists:reverse(tuple_to_list(Row)),
                                        translate_datatype(Statement, lists:reverse(NewRowR), NewClms);
                                    true ->
                                        translate_datatype(Statement, tuple_to_list(Row), NewClms)
                                end
                        end
                    , stmtRef  = Statement
                    , sortFun  = SortFun
                    , sortSpec = []}
     , RowIdAdded};
result_exec_query(RowIdError, OldStmt, Sql, Binds, NewSql, _RowIdAdded, Connection,
        SelectSections) when Sql =/= NewSql ->
    ?Info("result_exec_query rowid error ~p~n", [RowIdError]),
    ?Debug("RowIdError ~p", [RowIdError]),
    dpi_stmt_close(Connection, OldStmt),
    case dpi_conn_prepareStmt(Connection, Sql) of
        Stmt when is_reference(Stmt) ->
            ?Info("The statement ~p", [Stmt]),
            Result = bind_exec_stmt(Connection, Stmt, Binds),
            ?Info("The statement exec result ~p", [Result]),
            result_exec_query(Result, Stmt, Sql, Binds, Sql, false, Connection, SelectSections);
        {error, _DpiNifFile, _Line, #{message := Msg}} -> error(list_to_binary(Msg));
        Error -> error(Error)
    end;
result_exec_query(Error, Stmt, _Sql, _Binds, _NewSql, _RowIdAdded, Connection, _SelectSections) ->
    result_exec_error(Error, Stmt, Connection).

result_exec_stmt({rowids, _}, Statement, _Sql, _Binds, _NewSql, _RowIdAdded, _Connection, _SelectSections) ->
    io:format("result_exec_stmt v~p~n",[2]),
    dpi:stmt_release(Statement),
    ok;
result_exec_stmt({executed, _}, Statement, _Sql, _Binds, _NewSql, _RowIdAdded, _Connection, _SelectSections) ->
    io:format("result_exec_stmt v~p~n",[3]),
    dpi:stmt_release(Statement),
    ok;
result_exec_stmt({executed, 1, [{Var, Val}]}, Statement, Sql, {Binds, _}, NewSql, false, Conn, _SelectSections) ->
    io:format("result_exec_stmt v~p~n",[4]),
    dpi:stmt_release(Statement),
    case lists:keyfind(Var, 1, Binds) of
        {Var,out,'SQLT_RSET'} ->
            result_exec_stmt(Val:exec_stmt(), Val, Sql, undefined, NewSql, false, Conn, []);
        {Var,out,'SQLT_VNU'} ->
            {ok, [{Var, list_to_binary(oci_util:from_num(Val))}]};
        _ ->
            {ok, [{Var, Val}]}
    end;
result_exec_stmt({executed,_,Values}, Statement, _Sql, {Binds, _BindValues}, _NewSql, _RowIdAdded, Connection, _SelectSections) ->
    io:format("result_exec_stmt v~p~n",[5]),
    NewValues =
    lists:foldl(
      fun({Var, Val}, Acc) ->
              [{Var,
                case lists:keyfind(Var, 1, Binds) of
                    {Var,out,'SQLT_VNU'} -> list_to_binary(oci_util:from_num(Val));
                    _ -> Val
                end} | Acc]
      end, [], Values),
    ?Debug("Values ~p", [Values]),
    ?Debug("Binds ~p", [Binds]),
    dpi_stmt_close(Connection, Statement),
    {ok, NewValues}.

result_exec_error({error, _DpiNifFile, _Line, #{message := Msg}}, Statement, Connection) ->
    dpi_stmt_close(Connection, Statement),
    error(list_to_binary(Msg));
result_exec_error(Result, Statement, Connection) ->
    ?Error("Result with unrecognized format ~p", [Result]),
    dpi_stmt_close(Connection, Statement),
    error(Result).

-spec create_rowfun(boolean(), list(), term()) -> fun().
create_rowfun(RowIdAdded, Clms, Stmt) ->
    fun({{}, Row}) ->
            if
                RowIdAdded ->
                    [_|NewRowR] = lists:reverse(tuple_to_list(Row)),
                    translate_datatype(Stmt, lists:reverse(NewRowR), Clms);
                true ->
                    translate_datatype(Stmt, tuple_to_list(Row), Clms)
            end
    end.

expand_fields([<<"*">>], _, AllFields, Cols, Sections) ->
    NewFields = [lists:nth(N, AllFields) || N <- Cols],
    lists:keyreplace('fields', 1, Sections, {'fields', NewFields});
expand_fields(QryFields, Tables, AllFields, Cols, Sections) ->
    NormQryFlds = normalize_pt_fields(QryFields, #{}),
    LowerAllFields = [string:to_lower(binary_to_list(X)) || X <- AllFields],
    case can_expand(maps:keys(NormQryFlds), Tables, LowerAllFields) of
        true ->
            Keys = [lists:nth(N,LowerAllFields) || N <- Cols],
            NewFields = [maps:get(K, NormQryFlds) || K <- Keys],
            lists:keyreplace('fields', 1, Sections, {'fields',NewFields});
        false ->
            Sections
    end.

can_expand(LowerSelectFields, [TableName], LowerAllFields) when is_binary(TableName) ->
    length(LowerSelectFields) =:= length(LowerAllFields) andalso [] =:= (LowerSelectFields -- LowerAllFields);
can_expand(_, _, _) -> false.

normalize_pt_fields([], Result) -> Result;
normalize_pt_fields([{as, _Field, Alias} = Fld | Rest], Result) when is_binary(Alias) ->
    Normalized = string:to_lower(binary_to_list(Alias)),
    normalize_pt_fields(Rest, Result#{Normalized => Fld});
normalize_pt_fields([TupleField | Rest], Result) when is_tuple(TupleField) ->
    case element(1, TupleField) of
        'fun' ->
            BinField = sqlparse_fold:top_down(sqlparse_format_flat, TupleField, []),
            Normalized = string:to_lower(binary_to_list(BinField)),
            normalize_pt_fields(Rest, Result#{Normalized => TupleField});
        _ ->
            normalize_pt_fields(Rest, Result)
    end;
normalize_pt_fields([Field | Rest], Result) when is_binary(Field) ->
    Normalized = string:to_lower(binary_to_list(Field)),
    normalize_pt_fields(Rest, Result#{Normalized => Field});
normalize_pt_fields([_Ignored | Rest], Result) ->
    normalize_pt_fields(Rest, Result).

build_sort_spec(SelectSections, StmtCols) ->
    FullMap = build_full_map(StmtCols),
    case lists:keyfind('order by', 1, SelectSections) of
        {'order by', OrderBy} ->
            [process_sort_order(ColOrder, FullMap) || ColOrder <- OrderBy];
        _ ->
            []
    end.

process_sort_order({Name, <<>>}, Map) ->
    process_sort_order({Name, <<"asc">>}, Map);
process_sort_order({Name, Dir}, []) when is_binary(Name)-> {Name, Dir};
process_sort_order({Name, Dir}, [#bind{alias = Alias, cind = Pos} | Rest]) when is_binary(Name) ->
    case string:to_lower(binary_to_list(Name)) =:= string:to_lower(binary_to_list(Alias)) of
        true -> {Pos, Dir};
        false -> process_sort_order({Name, Dir}, Rest)
    end;
process_sort_order({Fun, Dir}, Map) ->
    process_sort_order({sqlparse_fold:top_down(sqlparse_format_flat, Fun, []), Dir}, Map).


%%% Model how imem gets the new filter and sort results %%%%
%       NewSortFun = imem_sql:sort_spec_fun(SortSpec, FullMaps, ColMaps),
%       %?Debug("NewSortFun ~p~n", [NewSortFun]),
%       OrderBy = imem_sql:sort_spec_order(SortSpec, FullMaps, ColMaps),
%       %?Debug("OrderBy ~p~n", [OrderBy]),
%       Filter =  imem_sql:filter_spec_where(FilterSpec, ColMaps, WhereTree),
%       %?Debug("Filter ~p~n", [Filter]),
%       Cols1 = case Cols0 of
%           [] ->   lists:seq(1,length(ColMaps));
%           _ ->    Cols0
%       end,
%       AllFields = imem_sql:column_map_items(ColMaps, ptree),
%       % ?Debug("AllFields ~p~n", [AllFields]),
%       NewFields =  [lists:nth(N,AllFields) || N <- Cols1],
%       % ?Debug("NewFields ~p~n", [NewFields]),
%       NewSections0 = lists:keyreplace('fields', 1, SelectSections, {'fields',NewFields}),
%       NewSections1 = lists:keyreplace('where', 1, NewSections0, {'where',Filter}),
%       %?Debug("NewSections1 ~p~n", [NewSections1]),
%       NewSections2 = lists:keyreplace('order by', 1, NewSections1, {'order by',OrderBy}),
%       %?Debug("NewSections2 ~p~n", [NewSections2]),
%       NewSql = sqlparse_fold:top_down(sqlparse_format_flat, {select,NewSections2}, []),     % sql_box:flat_from_pt({select,NewSections2}),
%       %?Debug("NewSql ~p~n", [NewSql]),
%       {ok, NewSql, NewSortFun}

filter_and_sort_internal(_Connection, FilterSpec, SortSpec, Cols, Query, StmtCols) ->
    FullMap = build_full_map(StmtCols),
    case Cols of
        [] ->   Cols1 = lists:seq(1,length(FullMap));
        _ ->    Cols1 = Cols
    end,
    % AllFields = imem_sql:column_map_items(ColMaps, ptree), %%% This should be the correct way if doing it.
    AllFields = [C#bind.alias || C <- FullMap],
    SortSpecExplicit = [{Col, Dir} || {Col, Dir} <- SortSpec, is_integer(Col)],
    NewSortFun = imem_sql_expr:sort_spec_fun(SortSpecExplicit, FullMap, FullMap),
    case sqlparse:parsetree(Query) of
        {ok,[{{select, SelectSections},_}]} ->
            {fields, Flds} = lists:keyfind(fields, 1, SelectSections),
            {from, Tables} = lists:keyfind(from, 1, SelectSections),
            {where, WhereTree} = lists:keyfind(where, 1, SelectSections),
            NewSections0 = expand_fields(Flds, Tables, AllFields, Cols1, SelectSections),
            Filter = imem_sql_expr:filter_spec_where(FilterSpec, FullMap, WhereTree),
            FilterEmptyAsNull = filter_replace_empty(Filter),
            NewSections1 = lists:keyreplace('where', 1, NewSections0, {'where',FilterEmptyAsNull}),
            OrderBy = imem_sql_expr:sort_spec_order(SortSpec, FullMap, FullMap),
            NewSections2 = lists:keyreplace('order by', 1, NewSections1, {'order by',OrderBy}),
            NewSql = sqlparse_fold:top_down(sqlparse_format_flat, {select, NewSections2}, []);
        _->
            NewSql = Query
    end,
    {ok, NewSql, NewSortFun}.

filter_replace_empty({'=', Column, <<"''">>}) -> {is, Column, <<"null">>};
filter_replace_empty({in, Column, {list, List}} = In) ->
    EmptyRemoved = [E || E <- List, E =/= <<"''">>],
    case length(EmptyRemoved) =:= length(List) of
        true -> In; % Nothing to do
        false -> {'or', {in, Column, {list, EmptyRemoved}}, {is, Column, <<"null">>}}
    end;
filter_replace_empty({Op, Parameter1, Parameter2}) ->
    {Op, filter_replace_empty(Parameter1), filter_replace_empty(Parameter2)};
filter_replace_empty(Condition) -> Condition.

-spec to_imem_type(atom()) -> atom().
to_imem_type('SQLT_NUM') -> number;
to_imem_type(_) -> binstr.

build_full_map(Clms) ->
    [#bind{ tag = list_to_atom([$$|integer_to_list(T)])
              , name = Alias
              , alias = Alias
              , tind = 2
              , cind = T
              , type = to_imem_type(OciType)
              , len = Len
              , prec = undefined }
     || {T, #stmtCol{alias = Alias, type = OciType, len = Len}} <- lists:zip(lists:seq(1,length(Clms)), Clms)].

build_sort_fun(_Sql, _Clms) ->
    fun(_Row) -> {} end.

-spec cols_to_rec([map()], list()) -> [#stmtCol{}].
cols_to_rec([], _) -> [];
cols_to_rec([#{
    name := AliasStr,
    typeInfo := #{
        oracleTypeNum := Type,
        fsPrecision := FsPrec
    }} | Rest], Fields
) when Type =:= 'DPI_ORACLE_TYPE_TIMESTAMP_TZ'; Type =:= 'DPI_ORACLE_TYPE_TIMESTAMP' ->
    Alias = list_to_binary(AliasStr),
    {Tag, ReadOnly, NewFields} = find_original_field(Alias, Fields),
    [#stmtCol{ tag = Tag
             , alias = Alias
             , type = Type
             , prec = FsPrec
             , readonly = ReadOnly} | cols_to_rec(Rest, NewFields)];
cols_to_rec([#{
    name := AliasStr,
    typeInfo := #{
        oracleTypeNum := 'DPI_ORACLE_TYPE_NUMBER',
        precision := 63,
        scale := -127 
    }} | Rest], Fields
) ->
    Alias = list_to_binary(AliasStr),
    {Tag, ReadOnly, NewFields} = find_original_field(Alias, Fields),
    [#stmtCol{ tag = Tag
             , alias = Alias
             , type = 'DPI_ORACLE_TYPE_NUMBER'
             , len = 19
             , prec = dynamic
             , readonly = ReadOnly} | cols_to_rec(Rest, NewFields)];
cols_to_rec([#{
    name := AliasStr,
    typeInfo := #{
        oracleTypeNum := 'DPI_ORACLE_TYPE_NUMBER',
        scale := -127 
    }} | Rest], Fields
) ->
    Alias = list_to_binary(AliasStr),
    {Tag, ReadOnly, NewFields} = find_original_field(Alias, Fields),
    [#stmtCol{ tag = Tag
             , alias = Alias
             , type = 'DPI_ORACLE_TYPE_NUMBER'
             , len = 38
             , prec = dynamic
             , readonly = ReadOnly} | cols_to_rec(Rest, NewFields)];
cols_to_rec([#{
    name := AliasStr,
    typeInfo := #{
        oracleTypeNum := Type
    }} | Rest], Fields
) when Type =:= 'DPI_ORACLE_TYPE_NATIVE_DOUBLE'; Type =:= 'DPI_ORACLE_TYPE_NATIVE_FLOAT' ->
    Alias = list_to_binary(AliasStr),
    {Tag, ReadOnly, NewFields} = find_original_field(Alias, Fields),
    [#stmtCol{ tag = Tag
             , alias = Alias
             , type = Type
             , readonly = ReadOnly} | cols_to_rec(Rest, NewFields)];
cols_to_rec([#{
    name := AliasStr,
    typeInfo := #{
        oracleTypeNum := Type, 
        clientSizeInBytes := Len,
        precision := Prec
    }} | Rest],
    Fields
) ->
    Alias = list_to_binary(AliasStr),
    {Tag, ReadOnly, NewFields} = find_original_field(Alias, Fields),
    [#stmtCol{ tag = Tag
             , alias = Alias
             , type = Type
             , len = Len
             , prec = Prec
             , readonly = ReadOnly} | cols_to_rec(Rest, NewFields)].

-spec get_alias([#stmtCol{}]) -> [binary()].
get_alias([]) -> [];
get_alias([#stmtCol{alias = A} | Rest]) ->
    [A | get_alias(Rest)].

translate_datatype(_Stmt, [], []) -> [];
translate_datatype(Stmt, [Null | RestRow], [#stmtCol{} | RestCols]) when Null =:= null; Null =:= <<>>->
    [<<>> | translate_datatype(Stmt, RestRow, RestCols)];
translate_datatype(Stmt, [R | RestRow], [#stmtCol{type = 'DPI_ORACLE_TYPE_TIMESTAMP_TZ'} | RestCols]) ->
    [dpi_to_dderltstz(R) | translate_datatype(Stmt, RestRow, RestCols)];
translate_datatype(Stmt, [R | RestRow], [#stmtCol{type = 'DPI_ORACLE_TYPE_TIMESTAMP'} | RestCols]) ->
    [dpi_to_dderlts(R) | translate_datatype(Stmt, RestRow, RestCols)];
translate_datatype(Stmt, [R | RestRow], [#stmtCol{type = 'DPI_ORACLE_TYPE_DATE'} | RestCols]) ->
    [dpi_to_dderltime(R) | translate_datatype(Stmt, RestRow, RestCols)];
translate_datatype(Stmt, [Number | RestRow], [#stmtCol{type = Type} | RestCols]) when
        Type =:= 'DPI_ORACLE_TYPE_NUMBER';
        Type =:= 'DPI_ORACLE_TYPE_NATIVE_DOUBLE';
        Type =:= 'DPI_ORACLE_TYPE_NATIVE_FLOAT' ->
    Result = dderloci_utils:clean_dynamic_prec(float_to_binary(Number, [{decimals,20}, compact])),
    [Result | translate_datatype(Stmt, RestRow, RestCols)];
translate_datatype(Stmt, [{_Pointer, Size, Path, Name} | RestRow], [#stmtCol{type = 'SQLT_BFILEE'} | RestCols]) ->
    SizeBin = integer_to_binary(Size),
    [<<Path/binary, $#, Name/binary, 32, $[, SizeBin/binary, $]>> | translate_datatype(Stmt, RestRow, RestCols)];
translate_datatype(Stmt, [{Pointer, Size} | RestRow], [#stmtCol{type = 'SQLT_BLOB'} | RestCols]) ->
    if
        Size > ?PREFETCH_SIZE ->
            {lob, Trunc} = Stmt:lob(Pointer, 1, ?PREFETCH_SIZE),
            SizeBin = integer_to_binary(Size),
            AsIO = imem_datatype:binary_to_io(Trunc),
            [<<AsIO/binary, $., $., 32, $[, SizeBin/binary, $]>> | translate_datatype(Stmt, RestRow, RestCols)];
        true ->
            {lob, Full} = Stmt:lob(Pointer, 1, Size),
            AsIO = imem_datatype:binary_to_io(Full),
            [AsIO | translate_datatype(Stmt, RestRow, RestCols)]
    end;
translate_datatype(Stmt, [Raw | RestRow], [#stmtCol{type = 'SQLT_BIN'} | RestCols]) ->
    [imem_datatype:binary_to_io(Raw) | translate_datatype(Stmt, RestRow, RestCols)];
translate_datatype(Stmt, [R | RestRow], [#stmtCol{} | RestCols]) ->
    [R | translate_datatype(Stmt, RestRow, RestCols)].

-spec fix_row_format(term(), [list()], [#stmtCol{}], boolean()) -> [tuple()].
fix_row_format(_Stmt, [], _, _) -> [];
fix_row_format(Stmt, [Row | Rest], Columns, ContainRowId) ->
    %% TODO: we have to add the table name at the start of the rows i.e
    %  rows [
    %        {{temp,1,2,3},{}},
    %        {{temp,4,5,6},{}}
    %  ]

    %% TODO: Convert the types to imem types??
    % db_to_io(Type, Prec, DateFmt, NumFmt, _StringFmt, Val),
    % io_to_db(Item,Old,Type,Len,Prec,Def,false,Val) when is_binary(Val);is_list(Val)
    if
        ContainRowId ->
            {RestRow, [RowId]} = lists:split(length(Row) - 1, Row),
            [{{}, list_to_tuple(fix_format(Stmt, RestRow, Columns) ++ [RowId])} | fix_row_format(Stmt, Rest, Columns, ContainRowId)];
        true ->
            [{{}, list_to_tuple(fix_format(Stmt, Row, Columns))} | fix_row_format(Stmt, Rest, Columns, ContainRowId)]
    end.

fix_format(_Stmt, [], []) -> [];
fix_format(Stmt, [<<0:8, _/binary>> | RestRow], [#stmtCol{type = 'SQLT_NUM'} | RestCols]) ->
    [null | fix_format(Stmt, RestRow, RestCols)];
fix_format(Stmt, [Number | RestRow], [#stmtCol{type = 'SQLT_NUM', len = Scale, prec = dynamic} | RestCols]) ->
    {Mantissa, Exponent} = dderloci_utils:oranumber_decode(Number),
    FormattedNumber = imem_datatype:decimal_to_io(Mantissa, Exponent),
    [imem_datatype:io_to_decimal(FormattedNumber, undefined, Scale) | fix_format(Stmt, RestRow, RestCols)];
fix_format(Stmt, [Number | RestRow], [#stmtCol{type = 'SQLT_NUM', len = Len,  prec = Prec} | RestCols]) ->
    {Mantissa, Exponent} = dderloci_utils:oranumber_decode(Number),
    FormattedNumber = imem_datatype:decimal_to_io(Mantissa, Exponent),
    [imem_datatype:io_to_decimal(FormattedNumber, Len, Prec) | fix_format(Stmt, RestRow, RestCols)];
fix_format(Stmt, [<<0, 0, 0, 0, 0, 0, 0, _/binary>> | RestRow], [#stmtCol{type = 'SQLT_DAT'} | RestCols]) -> %% Null format for date.
    [<<>> | fix_format(Stmt, RestRow, RestCols)];
fix_format(Stmt, [<<Date:7/binary, _/binary>> | RestRow], [#stmtCol{type = 'SQLT_DAT'} | RestCols]) -> %% Trim to expected binary size.
    [Date | fix_format(Stmt, RestRow, RestCols)];
fix_format(Stmt, [<<0,0,0,0,0,0,0,0,0,0,0,_/binary>> | RestRow], [#stmtCol{type = 'SQLT_TIMESTAMP'} | RestCols]) -> %% Null format for timestamp.
    [<<>> | fix_format(Stmt, RestRow, RestCols)];
fix_format(Stmt, [<<TimeStamp:11/binary, _/binary>> | RestRow], [#stmtCol{type = 'SQLT_TIMESTAMP'} | RestCols]) -> %% Trim to expected binary size.
    [TimeStamp | fix_format(Stmt, RestRow, RestCols)];
fix_format(Stmt, [<<0,0,0,0,0,0,0,0,0,0,0,0,0,_/binary>> | RestRow], [#stmtCol{type = 'SQLT_TIMESTAMP_TZ'} | RestCols]) -> %% Null format for timestamp.
    [<<>> | fix_format(Stmt, RestRow, RestCols)];
fix_format(Stmt, [<<TimeStampTZ:13/binary, _/binary>> | RestRow], [#stmtCol{type = 'SQLT_TIMESTAMP_TZ'} | RestCols]) -> %% Trim to expected binary size.
    [TimeStampTZ | fix_format(Stmt, RestRow, RestCols)];
fix_format(Stmt, [{Pointer, Size} | RestRow], [#stmtCol{type = 'SQLT_CLOB'} | RestCols]) ->
%% TODO: This is a workaround as there is no real support for CLOB in dderl or the current
%%       driver, so we read full text here and treat it as a normal STR, Oracle is smart
%%       to do the conversion into CLOB on the way in.
    {lob, Full} = Stmt:lob(Pointer, 1, Size),
    [Full | fix_format(Stmt, RestRow, RestCols)];
fix_format(Stmt, [Cell | RestRow], [#stmtCol{} | RestCols]) ->
    [Cell | fix_format(Stmt, RestRow, RestCols)].

-spec run_table_cmd(tuple(), atom(), binary()) -> ok | {error, term()}. %% %% !! Fix this to properly use statements.
run_table_cmd({oci_port, _, _} = _Connection, restore_table, _TableName) -> {error, <<"Command not implemented">>};
run_table_cmd({oci_port, _, _} = _Connection, snapshot_table, _TableName) -> {error, <<"Command not implemented">>};
run_table_cmd({oci_port, _, _} = Connection, truncate_table, TableName) ->
    run_table_cmd(Connection, iolist_to_binary([<<"truncate table ">>, TableName]));
run_table_cmd({oci_port, _, _} = Connection, drop_table, TableName) ->
    run_table_cmd(Connection, iolist_to_binary([<<"drop table ">>, TableName])).

-spec run_table_cmd(reference(), binary()) -> ok | {error, term()}.
run_table_cmd(Connection, SqlCmd) ->
    Stmt = dpi:conn_prepareStmt(Connection, false, SqlCmd, <<"">>),
    dpi:stmt_execute(Stmt, []),
    dpi:stmt_release(Stmt),
    ok.

-spec find_original_field(binary(), list()) -> {binary(), boolean(), list()}.
find_original_field(Alias, []) -> {Alias, false, []};
find_original_field(Alias, [<<"*">>]) -> {Alias, false, []};
find_original_field(Alias, [Field | Fields]) when is_binary(Field) ->
    compare_alias(Alias, Field, Fields, Field, {Alias, false, Fields});
find_original_field(Alias, [{as, Name, Field} = CompleteAlias | Fields])
  when is_binary(Name),
       is_binary(Field) ->
    compare_alias(Alias, Field, Fields, CompleteAlias, {Name, false, Fields});
find_original_field(Alias, [{as, _Expr, Field} = CompleteAlias | Fields])
  when is_binary(Field) ->
    compare_alias(Alias, Field, Fields, CompleteAlias, {Alias, true, Fields});
find_original_field(Alias, [Field | Fields]) ->
    {ResultName, ReadOnly, RestFields} = find_original_field(Alias, Fields),
    {ResultName, ReadOnly, [Field | RestFields]}.

-spec compare_alias(binary(), binary(), list(), term(), binary()) -> {binary(), boolean(), list()}.
compare_alias(Alias, Field, Fields, OrigField, Result) ->
    LowerAlias = string:to_lower(binary_to_list(Alias)),
    LowerField = string:to_lower(binary_to_list(Field)),
    AliasQuoted = [$" | LowerAlias] ++ [$"],
    if
        LowerAlias =:= LowerField -> Result;
        AliasQuoted =:= LowerField -> Result;
        true ->
            {ResultName, ReadOnly, RestFields} = find_original_field(Alias, Fields),
            {ResultName, ReadOnly, [OrigField | RestFields]}
    end.

-spec parse_sql(tuple(), binary()) -> {binary(), binary(), binary(), boolean(), list()}.
parse_sql({ok, [{{select, SelectSections},_}]}, Sql) ->
    {TableName, NewSql, RowIdAdded} = inject_rowid(select_type(SelectSections), SelectSections, Sql),
    {Sql, NewSql, TableName, RowIdAdded, SelectSections};
parse_sql({ok, [{{'begin procedure', _},_}]}, Sql) ->
    %% Old sql is replaced by the one with the correctly added semicolon, issue #401
    NewSql = append_semicolon(Sql, binary:last(Sql)),
    {NewSql, NewSql, <<"">>, false, []};
parse_sql(_UnsuportedSql, Sql) ->
    {Sql, Sql, <<"">>, false, []}.


%%%% Dpi data helper functions

dpi_to_dderltime(#{day := Day, month := Month, year := Year, hour := Hour, minute := Min, second := Sec}) ->
    iolist_to_binary([
        pad(Day), ".",
        pad(Month), ".",
        integer_to_list(Year), " ",
        pad(Hour), ":",
        pad(Min), ":", pad(Sec)
    ]).

dpi_to_dderlts(#{fsecond := FSecond} = DpiTs) ->
    ListFracSecs = case integer_to_list(FSecond) of
        NeedPad when length(NeedPad) < 9 -> pad(NeedPad, 9);
        FullPrec -> FullPrec
    end,
    case string:trim(ListFracSecs, trailing, "0") of
        [] -> dpi_to_dderltime(DpiTs);
        FracSecs -> iolist_to_binary([dpi_to_dderltime(DpiTs), $., FracSecs])
    end.

dpi_to_dderltstz(#{tzHourOffset := H,tzMinuteOffset := M} = DpiTsTz) ->
    iolist_to_binary([dpi_to_dderlts(DpiTsTz), format_tz(H, M)]).

format_tz(TZOffset, M) when TZOffset > 0 ->
    [$+ | format_tz_internal(TZOffset, M)];
format_tz(TZOffset, M) when TZOffset =:= 0, M >= 0 ->
    [$+ | format_tz_internal(TZOffset, M)];
format_tz(TZOffset, M) ->
    [$- | format_tz_internal(abs(TZOffset), abs(M))].

format_tz_internal(TZOffset, M) ->
    [pad_tz(TZOffset), integer_to_list(TZOffset), $:, pad_tz(M), integer_to_list(M)].

pad_tz(TzDigit) when TzDigit < 10 -> [$0];
pad_tz(_) -> [].

pad(ListValue, Size) ->
    lists:duplicate(Size - length(ListValue), $0) ++ ListValue.

pad(IntValue) ->
    Value = integer_to_list(IntValue),
    pad(Value, 2).

%%%% Dpi safe functions executed on dpi slave node

dpi_conn_prepareStmt(#odpi_conn{node = Node, connection = Conn}, Sql) ->
    dpi:safe(Node, fun() -> dpi:conn_prepareStmt(Conn, false, Sql, <<"">>) end).

dpi_stmt_execute(#odpi_conn{node = Node, connection = Conn}, Stmt) ->
    dpi:safe(Node, fun() ->
        Result = dpi:stmt_execute(Stmt, []),
        ok = dpi:conn_commit(Conn), % Commit automatically for any dderl query.
        Result
    end).

dpi_stmt_getInfo(#odpi_conn{node = Node}, Stmt) ->
    dpi:safe(Node, fun() -> dpi:stmt_getInfo(Stmt) end).

dpi_stmt_close(#odpi_conn{node = Node}, Stmt) ->
    dpi:safe(Node, fun() -> dpi:stmt_close(Stmt) end).

% This is not directly dpi but seems this is the best place to declare as it is rpc...
dpi_query_columns(#odpi_conn{node = Node}, Stmt, NColumns) ->
    dpi:safe(Node, fun() -> get_column_info(Stmt, 1, NColumns) end).

get_column_info(_Stmt, ColIdx, Limit) when ColIdx > Limit -> [];
get_column_info(Stmt, ColIdx, Limit) ->
    QueryInfoRef = dpi:stmt_getQueryInfo(Stmt, ColIdx),
    QueryInfo = dpi:queryInfo_get(QueryInfoRef),
    dpi:queryInfo_delete(QueryInfoRef),
    [QueryInfo | get_column_info(Stmt, ColIdx + 1, Limit)].

dpi_fetch_rows(#odpi_conn{node = Node}, Statement, BlockSize) ->
    dpi:safe(Node, fun() -> get_rows(Statement, BlockSize, []) end).

get_rows(_, 0, Acc) -> {lists:reverse(Acc), false};
get_rows(Stmt, NRows, Acc) ->
    case dpi:stmt_fetch(Stmt) of
        #{found := true} ->
            NumCols = dpi:stmt_getNumQueryColumns(Stmt),
            get_rows(Stmt, NRows -1, [get_column_values(Stmt, 1, NumCols) | Acc]);
        #{found := false} ->
            {lists:reverse(Acc), true}
    end.

get_column_values(_Stmt, ColIdx, Limit) when ColIdx > Limit -> [];
get_column_values(Stmt, ColIdx, Limit) ->
    #{data := Data} = dpi:stmt_getQueryValue(Stmt, ColIdx),
    Value = dpi:data_get(Data),
    dpi:data_release(Data),
    [Value | get_column_values(Stmt, ColIdx + 1, Limit)].
