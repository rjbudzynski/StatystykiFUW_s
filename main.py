

import marimo

__generated_with = "0.13.0"
app = marimo.App(width="medium")


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

    Tutaj ukryłem też kod zapewniający dostępność plików z danymi, na jakich będziemy operować.
    """)
    return mo, os, plt


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ## Tworzymy tabele z danych w plikach _parquet_

        Dla ułatwienia również pomocniczy widok (wirtualną tabelę) dat w jakich zmieniał się skład studentów. On nie jest konieczny, ale dzięki temu dalsze zapytania będą bardziej zwięzłe.
        """
    )
    return


@app.cell(hide_code=True)
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
    mo.md(r"""### Tabela pomocnicza, liczba studentów w czasie według programu i płci""")
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
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ### Przedstaw te dane na wykresie słupkowym

        Odróżnij programy zarządzane przez FUW od pozostałych. Użyj różnych kolorów dla tych dwóch grup. Dodaj legendę, tytuł i opisy osi. Użyj odpowiedniego rozmiaru wykresu, aby był czytelny.
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ### Stwórz tabelę z liczbami studentów aktualnie studiujących

        Z podziałem na programy.
        Przedstaw te dane na analogicznym wykresie słupkowym.
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ### Zrób wykres liniowy przedstawiający liczbę studentów i studentek w czasie

        Dla wszystkich programów fizyki (kody zawierają `FZ` i `NKF`), ale bez doktorantów.
        """
    )
    return


@app.cell
def _(liczby_studentow, mo, plt):
    _df = mo.sql(f"""
    select 
        DATA,
        sum(ILE_K) as ILE_K,
        sum(ILE_M) as ILE_M
    from 
        liczby_studentow
    where 
        (PRG_KOD like '%FZ%' or PRG_KOD like '%NKF%')
        and PRG_KOD not like 'DD%'
        and PRG_KOD not like 'SD%'
        and PRG_KOD not like 'SP%'
    group by
        DATA
    order by 
        DATA
    """)
    _f, _a = plt.subplots(figsize=(12, 7))
    _a.plot("DATA", "ILE_K", data=_df, label="Kobiety")
    _a.plot("DATA", "ILE_M", data=_df, label="Mężczyźni")
    _a.set_title("Liczba studentów i studentek fizyki w czasie")
    _a.legend()
    mo.center(_f)
    # _df
    return


if __name__ == "__main__":
    app.run()
