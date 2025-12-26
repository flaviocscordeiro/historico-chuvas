let
    // 1. Definição de Parâmetros
    DataInicial = #date(2007, 1, 1),
    DataFinal = Date.EndOfYear(Date.From(DateTime.LocalNow())),
    QtdeDias = Duration.Days(DataFinal - DataInicial) + 1,

    // 2. Criação da Lista Base
    FonteLista = List.Dates(DataInicial, QtdeDias, #duration(1, 0, 0, 0)),
    ConverterParaTabela = Table.FromList(FonteLista, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
    AlterarTipoParaData = Table.TransformColumnTypes(ConverterParaTabela,{{"Column1", type date}}),
    RenomearColunaData = Table.RenameColumns(AlterarTipoParaData,{{"Column1", "Data"}}),

    // 3. Adição de Colunas de Tempo
    AdicionarAno = Table.AddColumn(RenomearColunaData, "Ano", each Date.Year([Data]), Int64.Type),
    AdicionarMes = Table.AddColumn(AdicionarAno, "Mes", each Date.Month([Data]), Int64.Type),
    AdicionarNomeMes = Table.AddColumn(AdicionarMes, "MesNome", each Date.MonthName([Data], "pt-BR"), type text),
    AdicionarMesAbrev = Table.AddColumn(AdicionarNomeMes, "MesAbrev", each Text.Start([MesNome], 3), type text),
    AdicionarDia = Table.AddColumn(AdicionarMesAbrev, "Dia", each Date.Day([Data]), Int64.Type),
    AdicionarTrimestre = Table.AddColumn(AdicionarDia, "Trimestre", each Date.QuarterOfYear([Data]), Int64.Type),
    AdicionarSemanaAno = Table.AddColumn(AdicionarTrimestre, "SemanaAno", each Date.WeekOfYear([Data]), Int64.Type),

    // 4. Colunas de Ordenação (Sort)
    AdicionarMesAnoSort = Table.AddColumn(AdicionarSemanaAno, "MesAnoInt", each [Ano] * 100 + [Mes], Int64.Type),
    AdicionarMesAnoTexto = Table.AddColumn(AdicionarMesAnoSort, "MesAno", each [MesAbrev] & "/" & Text.End(Text.From([Ano]), 2), type text),

    // 5. Lógica de Negócio (Petrópolis)
    // Define estações (Março-Maio=Outono, etc.)
    AdicionarEstacao = Table.AddColumn(AdicionarMesAnoTexto, "Estacao", each 
        if [Mes] >= 3 and [Mes] <= 5 then "Outono" else
        if [Mes] >= 6 and [Mes] <= 8 then "Inverno" else
        if [Mes] >= 9 and [Mes] <= 11 then "Primavera" else "Verão"
    , type text),

    // Flag para filtrar meses de chuva intensa na serra (Dez, Jan, Fev, Mar)
    AdicionarFlagPeriodoCritico = Table.AddColumn(AdicionarEstacao, "EhPeriodoCritico", each 
        if [Mes] = 12 or [Mes] = 1 or [Mes] = 2 or [Mes] = 3 then "Sim" else "Não"
    , type text),

    // Agrupamento por década para análises históricas
    AdicionarDecada = Table.AddColumn(AdicionarFlagPeriodoCritico, "Decada", each 
        Text.From(Number.RoundDown([Ano]/10)*10) & "'s", type text
    )
in
    AdicionarDecada