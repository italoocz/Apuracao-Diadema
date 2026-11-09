#!/usr/bin/env python3
"""Calcula médias de votos nominais válidos usando processos e threads.

Uso: python analise_votacao.py [caminho.csv] [--municipio Diadema]
O CSV do TSE é delimitado por ponto e vírgula e contém um registo por linha.
"""

from __future__ import annotations

import argparse
import csv
import multiprocessing as mp
import os
import queue as thread_queue
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path


DEFAULT_CSV = Path(r"D:\ARQUIVOS\Downloads\votacao_candidato_munzona_2026_SP.csv")


@dataclass
class Totals:
    votes: int = 0
    rows: int = 0
    parties: dict[str, list[int]] = field(default_factory=dict)
    offices: dict[str, list[int]] = field(default_factory=dict)

    def add(self, party: str, office: str, votes: int) -> None:
        self.votes += votes
        self.rows += 1
        for groups, key in ((self.parties, party), (self.offices, office)):
            item = groups.setdefault(key or "(não informado)", [0, 0])
            item[0] += votes
            item[1] += 1

    def merge(self, other: "Totals") -> None:
        self.votes += other.votes
        self.rows += other.rows
        for target, source in ((self.parties, other.parties), (self.offices, other.offices)):
            for key, (votes, count) in source.items():
                item = target.setdefault(key, [0, 0])
                item[0] += votes
                item[1] += count


def byte_ranges(path: Path, workers: int) -> list[tuple[int, int]]:
    """Cria intervalos alinhados a linhas para que cada worker leia o seu trecho."""
    size = path.stat().st_size
    if not size:
        return []
    ranges = []
    with path.open("rb") as f:
        boundaries = [0]
        for i in range(1, workers):
            f.seek(size * i // workers)
            f.readline()  # termina o registo que começou antes do limite
            boundaries.append(f.tell())
        boundaries.append(size)
    for start, end in zip(boundaries, boundaries[1:]):
        if start < end:
            ranges.append((start, end))
    return ranges


def process_range(task: tuple[str, int, int, str | None]) -> Totals:
    filename, start, end, municipality = task
    result = Totals()
    with open(filename, "rb") as f:
        header_line = f.readline().decode("utf-8-sig", errors="replace").strip("\r\n")
        header = next(csv.reader([header_line], delimiter=";", quotechar='"'))
        indexes = {name: header.index(name) for name in
                   ("NM_MUNICIPIO", "SG_PARTIDO", "DS_CARGO", "QT_VOTOS_NOMINAIS_VALIDOS")}
        # O primeiro intervalo começa no cabeçalho; os restantes já começam
        # no limite de uma linha de dados calculado por byte_ranges().
        f.seek(f.tell() if start == 0 else start)
        while f.tell() < end:
            raw = f.readline()
            if not raw:
                break
            try:
                line = raw.decode("utf-8-sig", errors="replace").strip("\r\n")
                fields = next(csv.reader([line], delimiter=";", quotechar='"'))
                if not line:
                    continue
                municipality_name = fields[indexes["NM_MUNICIPIO"]]
                if municipality and municipality_name.strip().casefold() != municipality.casefold():
                    continue
                votes = int(fields[indexes["QT_VOTOS_NOMINAIS_VALIDOS"]])
                result.add(fields[indexes["SG_PARTIDO"]].strip(),
                           fields[indexes["DS_CARGO"]].strip(), votes)
            except (ValueError, IndexError, csv.Error) as exc:
                raise ValueError(f"Erro ao interpretar uma linha do CSV (perto do byte {f.tell()}).") from exc
    return result


def process_worker(task: tuple[str, int, int, str | None], output_queue) -> None:
    """Worker de processo; envia resultado ou erro pela Queue do multiprocessing."""
    try:
        output_queue.put((True, process_range(task)))
    except Exception as exc:
        output_queue.put((False, f"{type(exc).__name__}: {exc}"))


def thread_worker(task: tuple[str, int, int, str | None], output_queue) -> None:
    """Worker de thread; envia resultado ou erro pela Queue de threads."""
    try:
        output_queue.put((True, process_range(task)))
    except Exception as exc:
        output_queue.put((False, f"{type(exc).__name__}: {exc}"))


def combine_results(queue, expected: int) -> Totals:
    pieces = []
    errors = []
    for _ in range(expected):
        succeeded, value = queue.get()
        if succeeded:
            pieces.append(value)
        else:
            errors.append(value)
    if errors:
        raise RuntimeError("Falha(s) no processamento: " + "; ".join(errors))
    total = Totals()
    for piece in pieces:
        total.merge(piece)
    return total


def run_processes(tasks: list[tuple[str, int, int, str | None]]) -> Totals:
    """Cria processos explicitamente, usando start(), Queue e join()."""
    output_queue = mp.Queue()
    processes = [mp.Process(target=process_worker, args=(task, output_queue))
                 for task in tasks]
    try:
        for process in processes:
            process.start()
        return combine_results(output_queue, len(processes))
    finally:
        for process in processes:
            if process.pid is not None:
                process.join()
        output_queue.close()
        output_queue.join_thread()


def run_threads(tasks: list[tuple[str, int, int, str | None]]) -> Totals:
    """Cria threads explicitamente, usando start(), Queue e join()."""
    output_queue = thread_queue.Queue()
    threads = [threading.Thread(target=thread_worker, args=(task, output_queue))
               for task in tasks]
    try:
        for thread in threads:
            thread.start()
        return combine_results(output_queue, len(threads))
    finally:
        for thread in threads:
            if thread.ident is not None:
                thread.join()


def average_groups(groups: dict[str, list[int]]) -> list[tuple[str, float]]:
    return sorted(((key, votes / count) for key, (votes, count) in groups.items()),
                  key=lambda item: (-item[1], item[0]))


def print_result(label: str, result: Totals, seconds: float, municipality: str | None) -> None:
    scope = f"município de {municipality}" if municipality else "todos os registos do ficheiro"
    print(f"\n{label} — {scope}")
    if not result.rows:
        print("Nenhum registo encontrado para o município selecionado.")
        return
    print(f"Registos considerados: {result.rows:,}".replace(",", "."))
    print(f"Média geral de votos válidos: {result.votes / result.rows:.4f}")
    parties, offices = average_groups(result.parties), average_groups(result.offices)
    print(f"Partido com maior média: {parties[0][0]} ({parties[0][1]:.4f})")
    print(f"Cargo com maior média: {offices[0][0]} ({offices[0][1]:.4f})")
    print(f"Tempo total de execução: {seconds:.3f} s")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv", nargs="?", type=Path, default=DEFAULT_CSV,
                        help="ficheiro CSV (por omissão, ao lado do programa)")
    parser.add_argument("--municipio", default="Diadema",
                        help="filtra NM_MUNICIPIO (por omissão: Diadema); use --todos para o ficheiro inteiro")
    parser.add_argument("--todos", action="store_true", help="inclui todos os municípios do ficheiro")
    parser.add_argument("--workers", type=int, default=os.cpu_count() or 2,
                        help="número de processos/threads e blocos de leitura")
    args = parser.parse_args()
    if args.workers < 1:
        parser.error("--workers deve ser pelo menos 1")
    path = args.csv.resolve()
    if not path.is_file():
        parser.error(f"ficheiro não encontrado: {path}")
    municipality = None if args.todos else args.municipio
    tasks = [(str(path), start, end, municipality)
             for start, end in byte_ranges(path, args.workers)]
    if not tasks:
        parser.error("o ficheiro está vazio")

    started = time.perf_counter()
    proc_result = run_processes(tasks)
    proc_seconds = time.perf_counter() - started
    started = time.perf_counter()
    thread_result = run_threads(tasks)
    thread_seconds = time.perf_counter() - started

    print_result("MULTIPROCESSING", proc_result, proc_seconds, municipality)
    print_result("MULTITHREADING", thread_result, thread_seconds, municipality)
    if proc_seconds < thread_seconds:
        print("\nMelhor desempenho: multiprocessing (menor tempo).")
    elif thread_seconds < proc_seconds:
        print("\nMelhor desempenho: multithreading (menor tempo).")
    else:
        print("\nDesempenho empatado dentro da precisão medida.")


if __name__ == "__main__":
    mp.freeze_support()
    main()
