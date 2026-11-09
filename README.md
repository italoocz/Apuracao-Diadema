# Análise de votação — Diadema, SP

Programa Python para calcular a média de `QT_VOTOS_NOMINAIS_VALIDOS`, as médias
por `SG_PARTIDO` e por `DS_CARGO`, e comparar o tempo de execução com processos
e com threads. A versão com processos usa `multiprocessing.Process` e
`multiprocessing.Queue`; a versão com threads usa `threading.Thread`. Ambas
iniciam workers com `start()`, recolhem os resultados e aguardam com `join()`.

## Executar

Requer Python 3.10 ou superior. Usa apenas a biblioteca padrão; não precisa de
instalação de dependências.

### Ficheiro de dados

O CSV não está incluído neste repositório. Para obter os dados:

1. Aceda ao [Portal de Dados Abertos do TSE — Resultados 2026](https://dadosabertos.tse.jus.br/dataset/resultados-2026).
2. Na lista de recursos, descarregue o CSV de **Votação nominal por município e zona** (ou o recurso equivalente cujo nome de ficheiro seja `votacao_candidato_munzona_2026_SP.csv`).
3. Guarde o ficheiro no seu computador. Como ele é grande, não o envie para o GitHub junto com o código.
4. Ao executar, informe o caminho onde guardou o CSV.

### Comando

No PowerShell, substitua o caminho de exemplo pelo local onde guardou o CSV:

```powershell
python .\analise_votacao.py "C:\caminho\para\votacao_candidato_munzona_2026_SP.csv"
```

Em macOS ou Linux, por exemplo:

```bash
python3 analise_votacao.py "/caminho/para/votacao_candidato_munzona_2026_SP.csv"
```

Por omissão, considera apenas o município de Diadema, informado pela equipa.
Para calcular sobre todos os registos do ficheiro:

```powershell
python .\analise_votacao.py "C:\caminho\para\votacao_candidato_munzona_2026_SP.csv" --todos
```

É possível indicar outro município e controlar o número de workers:

```powershell
python .\analise_votacao.py "C:\caminho\para\votacao_candidato_munzona_2026_SP.csv" --municipio "São Paulo" --workers 4
```

O programa apresenta o número de registos usados, as médias, o vencedor por
partido e cargo e os tempos totais medidos para cada abordagem, além de indicar
qual terminou mais depressa. Cada worker abre o ficheiro e lê um intervalo
alinhado às linhas do CSV. Como a comparação depende do computador, do disco e
da quantidade de dados, a abordagem vencedora é determinada durante cada
execução.

**Equipa:** Gabriel Lacerda e José Italo — Diadema, SP.
