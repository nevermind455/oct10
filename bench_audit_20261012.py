"""Paired offline execution benchmark against the original Git HEAD.

Run after tests and other benchmarks finish, to avoid CPU contention:
    python bench_audit_20261012.py

Only the child benchmark's loopback exchange receives requests. Both versions
import testing_env, which disables .env and rejects external socket traffic.
The original source is extracted outside the working repository, without
copying local credentials or state. Raw logs and JSON remain in data/.
"""
from __future__ import annotations

import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import zipfile

ROOT = Path(__file__).resolve().parent
DATA = ROOT / 'data'
ARGS = ['--fair', '--pm', 'stress', '--orders', '1000', '--updates', '10000',
        '--sign', '300', '--journal', '300', '--wakeup', '200']


def measure(source: Path, label: str) -> dict:
    output = DATA / f'audit_20261012_{label}.json'
    log = DATA / f'audit_20261012_{label}.log'
    print(f'Running {label}: {source}', flush=True)
    with log.open('w', encoding='utf-8') as stream:
        subprocess.run([sys.executable, '-u', str(source / 'bench_execution.py'), *ARGS, '--json', str(output)],
                       cwd=source, env=dict(os.environ, PYTHONIOENCODING='utf-8'), stdout=stream,
                       stderr=subprocess.STDOUT, check=True, timeout=300)
    result = json.loads(output.read_text(encoding='utf-8'))
    live = result['live']
    # A timing from a failed workload is not a valid latency comparison.
    expected = 1100
    if (live['posted'] != expected or live['orders_sent'] != expected or live['halted']
            or live['skips'] or live['matched_unresolved'] or live['persistence_error']):
        raise RuntimeError(f'{label} workload incomplete: {live}')
    return result


def main() -> None:
    DATA.mkdir(exist_ok=True)
    revision = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()
    archive = subprocess.check_output(['git', 'archive', '--format=zip', revision], cwd=ROOT)
    with tempfile.TemporaryDirectory(prefix='sniperbtc-audit-original-') as work:
        source = Path(work).resolve()
        with zipfile.ZipFile(io.BytesIO(archive)) as zipped:
            for entry in zipped.infolist():
                target = (source / entry.filename).resolve()
                if not target.is_relative_to(source) or Path(entry.filename).name == '.env':
                    raise RuntimeError('unsafe archive path or tracked credential file')
            zipped.extractall(source)
        # One harness for both runs: the committed bench_execution.py gave its
        # fair-mode book no pre-move history, so the original code skipped all
        # 1100 orders. Only the benchmark driver is replaced, never the bot code.
        (source / 'bench_execution.py').write_bytes((ROOT / 'bench_execution.py').read_bytes())
        before = measure(source, 'before')
        after = measure(ROOT, 'after')
    pairs = {}
    for stage, original in before['live']['stages_ms'].items():
        current = after['live']['stages_ms'].get(stage)
        if original and current:
            pairs[stage] = {metric: {'before': original[metric], 'after': current[metric],
                                     'delta_ms': current[metric] - original[metric]}
                            for metric in ('p50', 'p95', 'p99', 'max')}
    report = {'original_revision': revision, 'workload_args': ARGS, 'comparison_ms': pairs,
              'before': before, 'after': after,
              'scope': 'Serial process-local loopback benchmarks; no real Polymarket orders.'}
    (DATA / 'audit_20261012_comparison.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
    for stage in ('trigger_to_send', 'trigger_to_ack_loopback', 'trigger_to_fill_known_loopback'):
        metric = pairs[stage]['p99']
        print(f"{stage} p99: {metric['before']:.3f} -> {metric['after']:.3f} ms")


if __name__ == '__main__':
    main()
