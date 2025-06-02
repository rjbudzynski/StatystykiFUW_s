import marimo

__generated_with = "0.13.15"
app = marimo.App(
    width="medium",
    layout_file="layouts/statystykiFUW.slides.json",
    sql_output="polars",
)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    # Przykład analizy danych: liczebność studentów fizyki (i&nbsp;pokrewnych) UW w czasie

    ## Narzędzia:
    - <a href=//docs.python.org target=_blank>Python</a> (oczywiście...)
    - <a href=//marimo.io target=_blank>Marimo</a> - nowa generacja notebooków dla Pythona
    - <a href=//duckdb.org target=_blank>DuckDB</a> - silnik SQL do celów analitycznych
    - <a href=//pola.rs target=_blank>Polars</a> - jak Pandas, tylko lepsze 😀
    - <a href=//matplotlib.org target=_blank>Matplotlib</a> - nie jest ideałem, ale wszyscy znają

    <div style="text-align:right;">&copy; 2025 RJ Budzyński</div>
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ### Jak najkrócej o SQL

    - SQL operuje na danych tabelarycznych: nazwane kolumny o określonym typie danych
    - zawiera polecenia wstawiania danych, ich modyfikacji, kwerendy, ...
    - nas interesują kwerendy

    Ogólna struktura kwerendy (zapytania):

    ```sql
    SELECT select_expressions ...
    FROM join_expression
    WHERE conditions
    GROUP BY group_keys
    ORDER BY order_keys
    ;
    ```

    Spore uproszczenie, ale tyle nam wystarczy.

    Prawie wszystkie elementy są opcjonalne, ale _kolejność musi być zachowana_.
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## Dane

    Niestety nie posłużę się danymi o charakterze wprost fizycznym. Związek danych jakie użyję z fizyką polega na tym, że dotyczą studentów Wydziału Fizyki 😀 Ale takie mam akurat pod ręką, i nadają się one do ilustracji pewnych metod.

    Źródłem danych jest USOS, zawierający wszelkie dane (m. in.) o programach studiów, studentach i przebiegu studiów, systematycznie tak mniej więcej od 2000 roku. Oczywiście dalece nie każdy może mieć wprost dostęp do bazy danych USOS, dlatego skorzystam z ekstraktu, zawierającego malutki podzbiór tych danych. Zadbałem o to, by ekstrakt ten był całkowicie wolny od jakichkolwiek danych osobowych podlegających ochronie RODO.

    W skrócie, posłużymy się dwiema tabelami:

    - `programy`, opisującą programy studiów w których prowadzeniu ma udział Wydział Fizyki. Przykładem programu jest `S1-FZ`: studia 1. stopnia z fizyki, ale również np. `SJ-MSMP`: studia jednolite magisterskie w Kolegium MISMaP.
    - `studenci`, opisującą osoby studiujące na programach. Najważniejsze pozycje w tej tabeli to kod programu, identyfikator osoby (`OS_ID`), płeć, data przyjęcia na program, data ukończenia (planowana lub już zaszła).

    Tabele te są zapakowane w pliki _parquet_. DuckDB potrafi czytać te pliki i interpretować ich zawartość jako tabele relacyjne. 

    Dlaczego akurat _parquet_? Jest to bardzo wygodny i kompaktowy format danych. Zachowuje on informacje o typach danych; np. daty są datami, a nie napisami do parsowania. Uwzględnia on kompresję danych, zoptymalizowaną dla danych tabelarycznych. Jeżeli mamy do czynienia z tabelą, w której w jednej lub więcej kolumn dane są w jakimś stopniu powtarzalne, tzn. liczba różnych wartości jest istotnie mniejsza niż liczba wierszy, to stopień kompresji potrafi być spektakularny.
    """
    )
    return


@app.cell(hide_code=True)
def _():
    import marimo as mo
    import matplotlib.pyplot as plt
    import polars as pl
    import os

    try:
        open("data/studenci.parquet", "rb")
        open("data/programy.parquet", "rb")
    except IOError:
        import requests

        try:
            os.mkdir("data")
        except FileExistsError:
            pass
        _studenci = requests.get(
            "https://budzynski.xyz/8$ZADJVA/studenci.parquet"
        ).content
        open("data/studenci.parquet", "wb").write(_studenci)
        _programy = requests.get(
            "https://budzynski.xyz/8$ZADJVA/programy.parquet"
        ).content
        open("data/programy.parquet", "wb").write(_programy)
    data_loaded = True

    mo.md("""
    ## Importy
    marimo, matplotlib

    Poza tym nie musimy nic importować explicite, natomiast szereg pakietów musi być zainstalowanych w środowisku w jakim tworzymy czy uruchamiamy ten notebook. Najlepiej posłużyć się narzędziem _uv_, a marimo nam w razie czego podpowie co doinstalować i pomoże w tym.

    W tej komórce ukryłem również kod zapewniający dostęp do danych, z których będziemy korzystać w dalszym ciągu.
    """)
    return mo, os, pl, plt


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## Tworzymy tabele z danych w plikach _parquet_

    Dla ułatwienia również pomocniczy widok (wirtualną tabelę) dat w jakich zmieniał się skład studentów. On nie jest konieczny, ale dzięki temu dalsze zapytania będą bardziej zwięzłe.
    """
    )
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        create or replace table studenci as select * from 'data/studenci.parquet' where PRG_KOD!='WUM';
        create or replace table programy as select * from 'data/programy.parquet' where PRG_KOD!='WUM';
        create or replace view daty as 
            select distinct PLAN_DATA_UKON as DATA 
            from studenci 
            where PLAN_DATA_UKON between '2000-10-01' and current_date
            union 
            select distinct DATA_PRZYJECIA as DATA 
            from studenci 
            where DATA_PRZYJECIA between '2000-10-01' and current_date;
        """,
        output=False
    )
    return daty, programy, studenci


@app.cell(hide_code=True)
def _(daty, mo, os, programy, studenci):
    _ile_osób = mo.sql(
        f"""
        select 
            count(distinct OS_ID) as "ILE OSÓB"
        from studenci
        ;
        """
    )
    _ile_k = mo.sql(
        f"""
        select 
            count(distinct OS_ID) as "ILE KOB"
        from studenci
        where PLEC='K'
        ;
        """
    )
    _ile_adm = mo.sql(
        f"""
        select 
            count(distinct OS_ID) as "ILE OSÓB"
        from studenci join programy using(PRG_KOD)
        where ADM
        ;"""
    )
    _ile_adm_k = mo.sql(
        f"""
        select
            count(distinct OS_ID) as "ILE OSÓB"
        from studenci join programy using (PRG_KOD)
        where 
            ADM and PLEC='K'
        ;"""
    )
    _ile_dat = mo.sql(
        f"""
        select 
            count(distinct DATA) as "ILE DAT"
        from daty
        ;
        """
    )
    mo.md(
        f"""
        ### Podsumowanie
        - W analizowanym okresie (od 2000-01-01 do dziś) studiowało na programach związanych z FUW łącznie {_ile_osób["ILE OSÓB"][0]} różnych osób.
        - Wśród nich było {_ile_k["ILE KOB"][0]} kobiet.
        - W tym na programach podlegających dziekanatowi FUW studiowało {_ile_adm["ILE OSÓB"][0]} różnych osób.
        - Wśród tych ostatnich było {_ile_adm_k["ILE OSÓB"][0]} kobiet.
        - Dane obliczono dla {_ile_dat["ILE DAT"][0]} różnych dat.

        Plik _studenci.parquet_ składa się z {mo.sql("select count(*) as ile from studenci;")["ile"][0]} rekordów, każdy z nich zawiera 6 pól. Natomiast rozmiar pliku to 
        {os.path.getsize("data/studenci.parquet")} bajtów. Ile bajtów przypada średnio na rekord (po kompresji)?
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Surowe dane""")
    return


@app.cell(hide_code=True)
def _(mo, studenci):
    _df = mo.sql(
        f"""
        SELECT * FROM studenci;
        """
    )
    mo.vstack([mo.md("### Tabela studentów"), _df])
    return


@app.cell(hide_code=True)
def _(mo, programy, studenci):
    programy_ile_os = mo.sql(
        f"""
        select 
            programy.*, count(distinct s.OS_ID) as "ILE OSÓB"
        from programy join studenci s on programy."PRG_KOD" = s.PRG_KOD
        group by all
        order by "ILE OSÓB" desc
        ;
        """
    )
    mo.vstack([mo.md(
        r"""
        ## Wszystkie programy, oraz liczby studiujących w całym rozważanym okresie

        Uwzględnione zostały wszystkie programy studiów, w których realizacji uczestniczy FUW - a więc np. wszystkie programy MISMaP. Wartość _true_ w kolumnie `ADM` oznacza, że dany program jest _zarządzany_ przez FUW, inaczej mówiąc (m. in.) &ndash; jego studenci są rozliczani przez dziekanat FUW.

        Liczby w kolumnie `ILE OSÓB` oznaczają, ile różnych osób studiowało na danym programie kiedykolwiek w ramach rozważanego okresu (od 2000-01-01 do dziś).
        """
    ), programy_ile_os])
    return (programy_ile_os,)


@app.cell
def _(plt, programy_ile_os):
    _f, _a = plt.subplots(figsize=(12, 7))
    _colors = ["c" if _adm else "m" for _adm in programy_ile_os["ADM"]]
    _labels = _colors[:]
    _labels[_colors.index("c")] = "zarządzane przez FUW"
    _labels[_colors.index("m")] = "współprowadzone przez FUW"
    _labels = ["_" if len(_x) == 1 else _x for _x in _labels]
    _a.bar(
        "PRG_KOD", "ILE OSÓB", data=programy_ile_os, color=_colors, label=_labels
    )
    _a.tick_params(axis="x", rotation=90, labelsize=7)
    _a.set_title("Liczba osób według programu studiów, kiedykolwiek")
    _a.legend()
    _a.grid(axis="y")
    _a.spines["top"].set_visible(False)
    _a.spines["right"].set_visible(False) 
    _f
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Liczby studiujących aktualnie z podziałem na programy""")
    return


@app.cell(hide_code=True)
def _(mo, programy, studenci):
    programy_ile_akt = mo.sql(
        f"""
        select 
            programy.*, count(distinct s.OS_ID) as "ILE OSÓB"
        from programy join studenci s on programy."PRG_KOD" = s.PRG_KOD
        where current_date between s."DATA_PRZYJECIA" and s."PLAN_DATA_UKON"
        group by 
            programy."PRG_KOD", programy."OPIS", programy."POCZATEK", programy."KONIEC", programy."ADM"
        order by "ILE OSÓB" desc
        ;
        """
    )
    return (programy_ile_akt,)


@app.cell
def _(mo, plt, programy_ile_akt):
    _f, _a = plt.subplots()
    _colors = ["m" if _adm else "c" for _adm in programy_ile_akt["ADM"]]
    _labels = _colors[:]
    _labels[_colors.index("m")] = "zarządzane przez FUW"
    _labels[_colors.index("c")] = "współprowadzone przez FUW"
    _labels = ["_" if len(_x) == 1 else _x for _x in _labels]
    _a.bar(
        "PRG_KOD", "ILE OSÓB", data=programy_ile_akt, color=_colors, label=_labels
    )
    _a.legend()
    _a.tick_params(axis="x", rotation=90, labelsize=7)
    _a.set_title("Liczba osób na programach studiów, aktualnie")
    _a.grid(axis="y")
    _a.spines["top"].set_visible(False)
    _a.spines["right"].set_visible(False)
    mo.hstack(
        [
            mo.sql(
                """select PRG_KOD, OPIS from programy_ile_akt order by "ILE OSÓB" desc"""
            ),
            _f,
        ],
        justify="center",
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Aktualni studenci z podziałem wg. programu i płci""")
    return


@app.cell(hide_code=True)
def _(mo, studenci):
    programy_ile_wg_plci = mo.sql(
        f"""
        select 
            PRG_KOD,
            count(case when PLEC='M' then 1 else NULL end) as ILE_M,
            count(case when PLEC='K' then 1 else NULL end) as ILE_K
        from studenci 
        where current_date between "DATA_PRZYJECIA" and "PLAN_DATA_UKON"
        group by "PRG_KOD"
        order by (ILE_K + ILE_M) desc
        ;
        """
    )
    return (programy_ile_wg_plci,)


@app.cell
def _(mo, plt, programy, programy_ile_wg_plci):
    _f, _a = plt.subplots()
    _a.bar(
        "PRG_KOD",
        "ILE_M",
        data=programy_ile_wg_plci,
        label="Mężczyźni",
        color="m",
    )
    _a.bar(
        "PRG_KOD",
        "ILE_K",
        bottom="ILE_M",
        data=programy_ile_wg_plci,
        label="Kobiety",
        color="c",
    )
    _a.legend()
    _a.tick_params(axis="x", rotation=90, labelsize=7)
    _a.grid(axis="y", lw=0.5, ls="--")
    _a.spines["top"].set_visible(False)
    _a.spines["right"].set_visible(False)
    _a.set_title("Liczba studentów aktualnych wg. programu i płci")
    _a.set_xlabel("kod programu")
    _a.set_ylabel("liczba studentów")
    _f.tight_layout()
    mo.hstack([mo.sql("select PRG_KOD, OPIS from programy"), _f], justify="center")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Tabela pomocnicza, zliczająca studentów wg. daty, programu i płci""")
    return


@app.cell
def _(daty, mo, studenci):
    liczby_studentow = mo.sql(
        f"""
        select 
            s.PRG_KOD,
            count(distinct s.OS_ID) as ILE,
            count(distinct case when s.PLEC='M' then s.OS_ID else NULL end) as ILE_M,
            count(distinct case when s.PLEC='K' then s.OS_ID else NULL end) as ILE_K,
            d.DATA
        from 
            studenci s join daty d 
                on d."DATA" between s."DATA_PRZYJECIA" 
                    and s."PLAN_DATA_UKON"
        group by 
            s."PRG_KOD", d."DATA"
        order by 
            d."DATA", s."PRG_KOD"
        ;
        """
    )
    return (liczby_studentow,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Liczba studentów w czasie, wszystkie programy z udziałem FUW""")
    return


@app.cell
def _(liczby_studentow, mo, plt):
    _df = mo.sql("""
    select 
        DATA,
        case when PRG_KOD similar to '(DD|SD).*' then 'doktoranci' else 'studenci' end as TYP,
        sum(ILE)::integer as ILE
    from liczby_studentow
    where PRG_KOD not like 'SP%'
    group by DATA, TYP
    order by DATA
    """)
    _f, _a = plt.subplots(figsize=(12, 7))
    _a.plot(
        "DATA",
        "ILE",
        data=_df.filter(_df["TYP"] == "studenci"),
        label="studenci",
        color="b",
        lw=0.5,
        marker="o",
        ms=1,
    )
    _a.plot(
        "DATA",
        "ILE",
        data=_df.filter(_df["TYP"] == "doktoranci"),
        label="doktoranci",
        color="r",
        lw=0.5,
        marker="o",
        ms=1,
    )
    _a.legend()
    _a.grid(lw=0.5, ls="--", axis="y")
    _a.spines["top"].set_visible(False)
    _a.spines["right"].set_visible(False)
    _a.set_title(
        "Liczba studentów i doktorantów programów (współ)prowadzonych przez FUW"
    )
    _a.set_xlabel("data")
    _a.set_ylabel("liczba osób")
    _f.tight_layout()
    _f
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Udział kobiet wśród studentów i doktorantów programów związanych z FUW""")
    return


@app.cell
def _(liczby_studentow, mo, pl, plt):
    _proc_k = mo.sql(
        """
    select 
        DATA,
        case when PRG_KOD similar to '(SD|DD).*' then 'doktoranci' else 'studenci' end as TYP,
        sum(ILE_K)::double / sum(ILE)::double *100 as PROC_K
    from
        liczby_studentow
    where
        PRG_KOD not like 'SP%'
    group by DATA, TYP
    order by DATA
        """
    )
    _f, _a = plt.subplots(figsize=(12, 7))
    for _typ in ("studenci", "doktoranci"):
        _a.plot(
            "DATA",
            "PROC_K",
            data=_proc_k.filter(pl.col("TYP")==_typ),
            label=_typ,
            lw=0.5,
            marker="o",
            ms=1,
        )
    _a.legend()
    _a.grid(lw=0.5, ls="--", axis="y")
    _a.spines["top"].set_visible(False)
    _a.spines["right"].set_visible(False)
    _a.set_title("Udział kobiet wśród studentów i doktorantów programów związanych z FUW")
    _a.set_xlabel("data")
    _a.set_ylabel("procent kobiet")
    _a.set_ylim(0, 100)
    _f
    return


@app.cell(hide_code=True)
def _(liczby_studentow, mo, programy):
    df_proc_k = mo.sql(
        f"""
        select 
            DATA,
            case when PRG_KOD similar to '(SD|DD).*' then 'doktoranci' else 'studenci' end as TYP,
            ADM,
            sum(ILE_K)::double / sum(ILE)::double *100 as PROC_K
        from
            liczby_studentow l join programy p using (PRG_KOD)
        where
            PRG_KOD not like 'SP%'
        group by all
        order by DATA
        ;
        """,
        output=False
    )
    return (df_proc_k,)


@app.cell(hide_code=True)
def _(mo):
    select_proc_k = mo.ui.dropdown(
        options={
            "Studenci kierunków zarządzanych przez FUW": "stud_adm",
            "Studenci kierunków współprowadzonych": "stud_nadm",
            "Doktoranci": "dokt",
        },
        value="Studenci kierunków zarządzanych przez FUW",
        label="Wybierz dane do pokazania:",
    )
    # mo.vstack(
    #     [
    #         mo.md("### Wybór danych"),
    #         select_proc_k,
    #     ]
    # )
    return (select_proc_k,)


@app.cell(hide_code=True)
def _(df_proc_k, mo, pl, plt, select_proc_k):
    _wybor = select_proc_k.value
    _filter = {
        "stud_adm": pl.col("ADM") & (pl.col("TYP") == "studenci"),
        "stud_nadm": ~pl.col("ADM") & (pl.col("TYP") == "studenci"),
        "dokt": pl.col("ADM") & (pl.col("TYP") == "doktoranci"),
    }
    _data = df_proc_k.filter(_filter[_wybor]).with_columns(
        hundred=100 - pl.col("PROC_K")
    )
    _f, _a = plt.subplots()
    _a.stackplot(
        "DATA",
        "hundred",
        "PROC_K",
        data=_data,
    )
    _a.spines[:].set_visible(False)
    _a.set_title(
        f"Podział płci wśród osób studiujących:\n{select_proc_k.selected_key}"
    )
    _a.legend(("M", "K"), loc=1)
    _a.set_xlabel("rok")
    _a.set_ylabel("procent")
    _f.tight_layout()
    mo.hstack(
        [
            mo.vstack(
                [
                    mo.md("### Wybór danych"),
                    select_proc_k,
                ]
            ),
            _f,
        ]
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Liczba studentów i doktorantów w czasie, programy zarządzane przez FUW""")
    return


@app.cell
def _(liczby_studentow, mo, pl, plt, programy):
    _df = mo.sql(
        """
    select 
        l.DATA,
        case when p.PRG_KOD similar to '(DD|SD).*' then 'doktoranci' else 'studenci' end as TYP,
        sum(l.ILE)::integer as ILE
    from liczby_studentow l join programy p on l.PRG_KOD = p.PRG_KOD
    where p.PRG_KOD not similar to 'SP.*'
        and p.ADM=true
    group by l.DATA, TYP
    order by l.DATA
        """
    )
    _f, _a = plt.subplots(figsize=(12, 7))
    for _typ in ("studenci", "doktoranci"):
        _a.plot(
            "DATA",
            "ILE",
            data=_df.filter(pl.col("TYP") == _typ),
            label=_typ,
            lw=0.5,
            marker="o",
            ms=1,
        )
    _a.legend()
    _a.grid(lw=0.5, ls="--", axis="y")
    _a.spines["top"].set_visible(False)
    _a.spines["right"].set_visible(False)
    _a.set_title("Liczba studentów i doktorantów programów zarządzanych przez FUW")
    _a.set_xlabel("data")
    _a.set_ylabel("liczba osób")
    _f
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## Liczba studentów wybranych kierunków studiów na FUW

    Z uwzględnieniem 1. i 2. stopnia, oraz studiów jednolitych magisterskich (póki istniały).
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    kierunki = mo.ui.multiselect(
        [
            "fizyka",
            "astronomia",
            "nanoinżynieria",
            "fbm",
            "optometria",
        ],
        ["fizyka"],
        label="Wybierz kierunki do wyświetlenia",
    ).form(submit_button_label="zastosuj")
    return (kierunki,)


@app.cell(hide_code=True)
def _(liczby_studentow, mo):
    liczby_kierunkow = mo.sql(
        f"""
        select 
            DATA,
            sum(case when PRG_KOD similar to '.*FZ.*' then ILE else NULL end)::INT as ILE_FZ,
            sum(case when PRG_KOD similar to '.*AS.*' then ILE else NULL end)::INT as ILE_AS,
            sum(case when PRG_KOD similar to '.*(INZN|NIN).*' then ILE else NULL end)::INT as ILE_IN,
            sum(case when PRG_KOD similar to '.*FBM' then ILE else NULL end)::INT as ILE_FBM,
            sum(case when PRG_KOD similar to '.*(OP.*|ESOO)' then ILE else NULL end)::INT as ILE_OP
        from liczby_studentow
        where PRG_KOD not similar to '(DD|SD|SP).*'
        group by DATA
        order by DATA
        ;
        """
    )
    return (liczby_kierunkow,)


@app.cell(hide_code=True)
def _(kierunki, mo):
    mo.vstack([mo.md("### Wybór kierunków"), kierunki])
    return


@app.cell
def _(kierunki, liczby_kierunkow, plt):
    _k = kierunki.value or ["fizyka"]
    kolumny = {
        "fizyka": "ILE_FZ",
        "astronomia": "ILE_AS",
        "nanoinżynieria": "ILE_IN",
        "fbm": "ILE_FBM",
        "optometria": "ILE_OP",
    }
    barwy = {
        "fizyka": "m",
        "astronomia": "y",
        "nanoinżynieria": "g",
        "fbm": "r",
        "optometria": "b",
    }

    _f, _a = plt.subplots(figsize=(12, 7))

    for _kierunek in _k:
        _a.plot(
            "DATA",
            kolumny[_kierunek],
            data=liczby_kierunkow,
            label=_kierunek,
            color=barwy[_kierunek],
            lw=0.5,
            marker="o",
            ms=1,
        )


    _a.legend()
    _a.grid(lw=0.5, ls="--", axis="y")
    _a.spines["top"].set_visible(False)
    _a.spines["right"].set_visible(False)
    _a.set_title("Liczba studentów wybranych kierunków studiów na FUW")
    _a.set_xlabel("data")
    _a.set_ylabel("liczba osób")
    _f.tight_layout()
    _f
    return barwy, kolumny


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Linie trendu - średnie roczne""")
    return


@app.cell(hide_code=True)
def _(liczby_studentow, mo):
    procent_kobiet = mo.sql(
        f"""
        with PK as (select
            DATA,
            sum(case when PRG_KOD similar to '.*FZ.*' then ILE_K else NULL end) / 
                sum(case when PRG_KOD similar to '.*FZ.*' then ILE else NULL end) * 100 as PROC_K_FZ,
            sum(case when PRG_KOD similar to '.*AS.*' then ILE_K else NULL end) /
                sum(case when PRG_KOD similar to '.*AS.*' then ILE else NULL end) * 100 as PROC_K_AS,
            sum(case when PRG_KOD similar to '.*(INZN|NIN).*' then ILE_K else NULL end) /
                sum(case when PRG_KOD similar to '.*(INZN|NIN).*' then ILE else NULL end) * 100 as PROC_K_IN,
            sum(case when PRG_KOD similar to '.*FBM.*' then ILE_K else NULL end) /
                sum(case when PRG_KOD similar to '.*FBM.*' then ILE else NULL end) * 100 as PROC_K_FBM,
            sum(case when PRG_KOD similar to '.*(OP.*|ESOO).*' then ILE_K else NULL end) /
                sum(case when PRG_KOD similar to '.*(OP.*|ESOO).*' then ILE else NULL end) * 100 as PROC_K_OP
            from liczby_studentow
            group by DATA
            order by DATA
            )
        select 
            YEAR(time_bucket('1 year', DATA, DATE '2000-10-01')) as ROK,
            AVG(PROC_K_FZ) as AVG_FZ,
            AVG(PROC_K_AS) as AVG_AS,
            AVG(PROC_K_IN) as AVG_IN,
            AVG(PROC_K_FBM) as AVG_FBM,
            AVG(PROC_K_OP) as AVG_OP
        from PK
        group by ROK
        order by ROK
        ;
        """
    )
    return (procent_kobiet,)


@app.cell
def _(barwy, kierunki, kolumny, liczby_kierunkow, mo, plt):
    _df = mo.sql(
        """
        SELECT
            YEAR(time_bucket('1 year', DATA, DATE '2000-10-01')) as ROK,
            --YEAR(DATA) AS ROK,
            AVG(ILE_FZ) as ILE_FZ,
            AVG(ILE_AS) as ILE_AS,
            AVG(ILE_IN) as ILE_IN,
            AVG(ILE_FBM) as ILE_FBM,
            AVG(ILE_OP) as ILE_OP
        FROM
            liczby_kierunkow
        GROUP BY
            ROK
        ORDER BY
            ROK;
        """
    )
    _k = kierunki.value or ["fizyka"]
    _f, _a = plt.subplots(figsize=(12, 7))

    for _kierunek in _k:
        _a.plot(
            "ROK",
            kolumny[_kierunek],
            data=_df,
            label=_kierunek,
            color=barwy[_kierunek],
            marker="o",
            ms=3,
        )

    _a.set_ylim(0, None)
    _a.legend()
    _a.grid(lw=0.5, ls="--", axis="y")
    _a.spines["top"].set_visible(False)
    _a.spines["right"].set_visible(False)
    _a.set_title(
        "Średnia roczna liczba studentów wybranych kierunków studiów na FUW"
    )
    _a.set_xlabel("rok akademicki")
    _a.set_ylabel("liczba osób")
    _f.tight_layout()
    _f
    return


@app.cell
def _(plt, procent_kobiet):
    _f, _a = plt.subplots(figsize=(12, 7))
    _a.plot(
        "ROK",
        "AVG_FZ",
        data=procent_kobiet,
        label="fizyka",
        color="m",
        lw=1.5,
        marker='o',
        ms=3,
    )
    _a.plot(
        "ROK",
        "AVG_AS",
        data=procent_kobiet,
        label="astronomia",
        color="y",
        lw=1.5,
        marker='o',
        ms=3,
    )
    _a.plot(
        "ROK",
        "AVG_IN",
        data=procent_kobiet,
        label="nanoinżynieria",
        color="g",
        lw=1.5,
        marker='o',
        ms=3,
    )
    _a.plot(
        "ROK",
        "AVG_FBM",
        data=procent_kobiet,
        label="fbm",
        color="r",
        lw=1.5,
        marker='o',
        ms=3,
    )
    _a.plot(
        "ROK",
        "AVG_OP",
        data=procent_kobiet,
        label="optometria",
        color="c",
        lw=1.5,
        marker='o',
        ms=3,
    )
    _a.legend()
    _a.grid(lw=0.5, ls="--", axis="y")
    _a.spines["top"].set_visible(False)
    _a.spines["right"].set_visible(False)
    _a.set_title("Procent kobiet wśród studentów głównych kierunków studiów na FUW")
    _a.set_xlabel("rok")
    _a.set_ylabel("procent kobiet")
    _a.set_ylim(0, 100)
    _f.tight_layout()
    _f
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## Czas trwania studiów
    Rozumiany jako okres jaki minął do uzyskania dyplomu od przyjęcia studenta _na ten sam program studiów, na którym ten dyplom uzyskał_.
    """
    )
    return


@app.cell
def _(mo, programy, studenci):
    miesiace_studiow = mo.sql(
        f"""
        select 
            PRG_KOD,
            STATUS,
            date_diff('month', DATA_PRZYJECIA, PLAN_DATA_UKON) as "MIESIĄCE NA STUDIACH"
        from studenci join programy using (PRG_KOD)
        where
            -- STATUS='DYP'
            PRG_KOD similar to '(S1|S2|DZ|DU).*'
            and ADM=true
        ;
        """
    )
    return (miesiace_studiow,)


@app.cell
def _(miesiace_studiow, mo, plt):
    _s1_fz = mo.sql("""
        select "MIESIĄCE NA STUDIACH", count(*) as ILE 
        from miesiace_studiow 
        where PRG_KOD similar to '(S1|DZ)-.*'
            and STATUS='DYP'
        group by "MIESIĄCE NA STUDIACH"
        order by "MIESIĄCE NA STUDIACH"
        """)  # [
    #     "MIESIĄCE NA STUDIACH"
    # ]
    _avg = mo.sql("""
        select 
            avg("MIESIĄCE NA STUDIACH") as AVG,
            median("MIESIĄCE NA STUDIACH") as MED,
            stddev("MIESIĄCE NA STUDIACH") as STD
        from miesiace_studiow 
        where PRG_KOD similar to '(S1|DZ)-.*'
            and STATUS='DYP'
        """)
    # print(_avg)
    plt.bar("MIESIĄCE NA STUDIACH", "ILE", data=_s1_fz, color="c")
    # plt.hist(
    #     _s1_fz, bins=40, color="c", label="studia I stopnia"
    # )
    plt.title("Czas trwania studiów I stopnia na FUW zakończonych dyplomem")
    plt.xlim(0, 80)
    plt.xlabel("miesiące")
    plt.ylabel("liczba studentów")
    plt.gcf().set_size_inches((12, 7))
    plt.tight_layout()
    mo.vstack([
        plt.gcf(),
        mo.md(f"""
        - Średnia: {_avg['AVG'][0]:.1f} miesięcy
        - Mediana: {_avg['MED'][0]:.1f} miesięcy
        - Odchylenie standardowe: {_avg['STD'][0]:.1f} miesięcy
        """)
    ])
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## Ile mija czasu pomiędzy pierwszym rozpoczęciem studiów a uzyskaniem dyplomu?
    To jest, pomiędzy datą przyjęcia studenta na _jakikolwiek_ program studiów _związany_ z FUW, a uzyskaniem dyplomu na _jakimkolwiek_ programie _zarządzanym przez FUW_.
    """
    )
    return


@app.cell
def _(mo, programy, studenci):
    miesiace_do_dyplomu = mo.sql(
        f"""
        with zdyplomem as (
            select studenci.* 
            from studenci join programy using (PRG_KOD)
            where 
                programy.ADM=true
                and PRG_KOD not like '(DD|SD|SP|MOST)%'
                and STATUS='DYP'
        ), pierwszestudia as (
            select 
                z.OS_ID,
                min(s.DATA_PRZYJECIA) as POCZATEK
            from zdyplomem z join studenci s using (OS_ID)
            group by z.OS_ID        
        ), miesiace as (
            select
                z.OS_ID,
                z.PRG_KOD,
                date_diff('month', p.POCZATEK, z.PLAN_DATA_UKON) as "MIESIĄCE NA STUDIACH",
            from 
                zdyplomem z join pierwszestudia p using (OS_ID)
            group by
                all
        )
        select 
            PRG_KOD, "MIESIĄCE NA STUDIACH", count(distinct OS_ID) as "LICZBA STUDENTÓW"
        from miesiace
            group by all
            order by "MIESIĄCE NA STUDIACH" asc
        ;
        """
    )
    return (miesiace_do_dyplomu,)


@app.cell
def _(miesiace_do_dyplomu, pl, plt):
    _s1 = miesiace_do_dyplomu.filter(
        pl.col("PRG_KOD").str.contains("^(S2|DU)")
    ).sort("MIESIĄCE NA STUDIACH")
    _f, _a = plt.subplots(figsize=(12, 7))
    _a.bar("MIESIĄCE NA STUDIACH", "LICZBA STUDENTÓW", data=_s1, color="c")
    _a.set_xlim(0, 100)
    _f.suptitle("Czas trwania studiów II stopnia na FUW zakończonych dyplomem")
    _a.set_title("Od rozpoczęcia studiów po raz pierwszy")
    _a.set_xlabel("miesiące")
    _a.set_ylabel("liczba studentów")
    _a.spines["top"].set_visible(False)
    _a.spines["right"].set_visible(False)
    _a.grid(lw=0.5, ls="--", axis="y")
    _f
    return


@app.cell
def _(mo):
    liczby_wg_programu_statusu = mo.sql(
        f"""
        with tab as (
            select 
                PRG_KOD,
                STATUS,
                count(distinct OS_ID) as ILE
            from studenci join programy using (PRG_KOD)
                where ADM
                    and PLAN_DATA_UKON<today()
                    and PRG_KOD not similar to '(DD|SD|MOST|SP).*'
            group by
                PRG_KOD,
                STATUS 
            order by PRG_KOD
        )
        pivot tab 
        on STATUS
        using ifnull(sum(ILE)::integer, 0)
        ;
        """
    )
    return (liczby_wg_programu_statusu,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Wyniki studiowania""")
    return


@app.cell
def _(liczby_wg_programu_statusu, pl, plt):
    _df = liczby_wg_programu_statusu.sort(pl.col("DYP") + pl.col("SKR"), descending=True)
    _f, _a = plt.subplots(figsize=(12, 7))
    _a.bar("PRG_KOD", "DYP", data=_df, label="dyplomy")
    _a.bar(
        "PRG_KOD",
        "SKR",
        data=liczby_wg_programu_statusu,
        bottom=liczby_wg_programu_statusu["DYP"],
        label="skreśleni",
    )
    _a.tick_params(axis="x", labelsize=7, rotation=60)
    _a.legend()
    _a.spines["top"].set_visible(False)
    _a.spines["right"].set_visible(False)
    _a.grid(lw=0.5, ls="--", axis="y")
    _a.set_title("Jak się kończą studia, w zależności od programu - cały badany okres")
    _f
    return


@app.cell(hide_code=True)
def _(mo, wybor_kierunkow):
    dyplomy = mo.sql(
        f"""
        with _tab as (
            select 
                year(PLAN_DATA_UKON - interval 9 month) as rok, 
                case when PRG_KOD similar to '(DZ|S1).*'
                    then 'LIC'
                    when PRG_KOD similar to '(DU|DM|S2).*'
                    then 'MGR'
                end as "typ dyplomu",
                count(distinct OS_ID) as ILE
            from 
                studenci join programy using (PRG_KOD)
            where
                ADM
                and STATUS='DYP'
                and PRG_KOD similar to '{wybor_kierunkow.value}'
            group by
                rok, "typ dyplomu"    
            order by 
                rok, "typ dyplomu"
        )
        pivot _tab
        on "typ dyplomu"
        using ifnull(first(ILE), 0)
        """,
        output=False
    )
    return (dyplomy,)


@app.cell(hide_code=True)
def _(dyplomy, mo, plt, wybor_kierunkow):
    _f, _a = plt.subplots(figsize=(12, 6))
    _a.bar("rok", "LIC", data=dyplomy, label="licencjat")
    _a.bar("rok", "MGR", bottom="LIC", data=dyplomy, label="magisterium")
    _a.legend()
    _a.spines["top"].set_visible(False)
    _a.spines["right"].set_visible(False)
    _a.grid(lw=0.5, ls="--", axis="y")
    _a.set_ylim((0, 240))
    _a.set_xlim((1999, 2025))
    _a.set_title(f"Dyplomy uzyskane na programach FUW wg. roku (akademickiego): {wybor_kierunkow.selected_key}")
    mo.output.append(wybor_kierunkow)
    mo.output.append(_f)
    return


@app.cell(hide_code=True)
def _(mo):
    wybor_kierunkow = mo.ui.dropdown(
        options={
            "wszystkie kierunki": ".*",
            "fizyka": ".*(FZ|NKF).*",
            "astronomia": ".*AS.*",
            "nanoinżynieria": ".*IN.*",
            "fizyka w biologii i medycynie": ".*FBM",
            "optometria": ".*(OP|OO).*",
        },
        value="wszystkie kierunki",
        label="Wybór kierunków: ",
    )
    return (wybor_kierunkow,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Rekrutacja""")
    return


@app.cell
def _(mo):
    rekrutacja_wg_programu = mo.sql(
        f"""
        with T as (
            select 
                year(DATA_PRZYJECIA) as rok, 
                case 
                    when PRG_KOD similar to '(DZ|S1).*'
                        then '1. stopień'
                    when PRG_KOD similar to '(DU|S2).*'
                        then '2. stopień'
                    when PRG_KOD like 'DM%'
                        then 'jednolite mgr.'
                end as "tryb studiów", 
                ifnull(count(distinct OS_ID), 0) as "liczba studentów"
            from studenci s
            where exists (
                select 1
                from programy p
                where p.PRG_KOD=s.PRG_KOD 
                    and ADM
                )
                and rok>1999
                and "tryb studiów" is not null
                and month(DATA_PRZYJECIA)=10
            group by 
                rok, "tryb studiów"
            order by rok
            )
        pivot T
        on "tryb studiów"
        using(first("liczba studentów"))
        ;
        """
    )
    return (rekrutacja_wg_programu,)


@app.cell
def _(pl, plt, rekrutacja_wg_programu):
    _f, _a = plt.subplots(figsize=(12, 7))
    _a.bar(
        "rok",
        "1. stopień",
        data=rekrutacja_wg_programu,
    )
    _a.bar(
        "rok",
        "2. stopień",
        bottom="1. stopień",
        data=rekrutacja_wg_programu,
    )
    _a.bar(
        "rok",
        "jednolite mgr.",
        bottom=rekrutacja_wg_programu.select(
            (pl.col("1. stopień") + pl.col("2. stopień")).alias("b")
        )["b"],
        data=rekrutacja_wg_programu,
    )
    _a.legend(("1. stopień", "2. stopień", "jednolite mgr."))
    _a.set_title("Rekrutacja na studia FUW w czasie wg. trybu studiów")
    _a.set_xticks(ticks=range(2000, 2025))
    _a.tick_params(axis="x", labelrotation=45)
    _a.spines["top"].set_visible(False)
    _a.spines["right"].set_visible(False)
    _a.grid(lw=0.5, ls="--", axis="y")
    _f.tight_layout()
    _f
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Skreślenia ze studiów według miesiąca""")
    return


@app.cell
def _(mo, programy, studenci):
    skreslenia_wg_miesiaca = mo.sql(
        f"""
        select 
            month(PLAN_DATA_UKON) as "miesiąc",
            count(distinct OS_ID) as ILE 
        from 
            studenci join programy using (PRG_KOD)
        where 
            ADM
            and PRG_KOD not similar to '(DD|SD|SP).*'
            and PLAN_DATA_UKON<today()
        group by 
            "miesiąc"
        order by
            "miesiąc"
        ;
        """
    )
    return (skreslenia_wg_miesiaca,)


@app.cell
def _(plt, skreslenia_wg_miesiaca):
    _f, _a = plt.subplots(figsize=(12, 7))
    _a.bar("miesiąc", "ILE", data=skreslenia_wg_miesiaca)
    _a.set_xticks(
        ticks=range(1, 13),
        labels=(
            "STY",
            "LUT",
            "MAR",
            "KWI",
            "MAJ",
            "CZE",
            "LIP",
            "SIE",
            "WRZ",
            "PAŹ",
            "LIS",
            "GRU",
        ),
    )
    _a.set_title("Skreślenia ze studiów w zależności od miesiąca")
    _a.spines["top"].set_visible(False)
    _a.spines["right"].set_visible(False)
    _a.grid(lw=0.5, ls="--", axis="y")
    _f.tight_layout()
    _f
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## Wyzwania

    Spróbować wyznaczyć i stworzyć wizualizacje odpowiedzi na niektóre lub wszystkie z poniższych pytań: 

    - Jak zmieniał się w czasie procent kobiet wśród osób studiujących w zależności od kierunku studiów, w postaci uśrednionej po roku akademickim?
    - Jak zmieniała się rekrutacja z czasem? W zależności od programu, bądź grupy programu (kierunek, tryb studiów)
    - W przypadku skreślenia/rezygnacji, po jakim czasie to następuje? Jak to zależy od programu/kierunku?
    - Jak często studenci kończą studia dyplomem, a jak często skreśleniem? Jak to się rozkłada w czasie od rozpoczęcia studiów, jak zależy od programu/kierunku/płci studenta?
    - Jak to zależy od programu studiów, płci?
    - Jak się to zmieniało w czasie, zależnie od roku rozpoczęcia studiów?
    - Ile mija czasu między wstąpieniem studenta na studia po raz pierwszy a ich zakończeniem?
    - Jakie inne ciekawe pytania można by postawić tym danym?
    """
    )
    return


if __name__ == "__main__":
    app.run()
