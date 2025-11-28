"""Geração e upload de documentos MEI sintéticos para testes."""

import argparse
import importlib
import os
import random
import string
import sys
import uuid
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable, List, Sequence

BASE_DIR = Path(__file__).resolve().parents[1]
SERVICES_PATH = BASE_DIR / "services"
if str(SERVICES_PATH) not in sys.path:
    sys.path.append(str(SERVICES_PATH))


@dataclass
class SyntheticDoc:
    doc_id: str
    tenant_id: str
    kind: str
    text: str
    has_error: bool = False


_SERVICES: Sequence[str] = (
    "Consultoria MEI",
    "Design gráfico",
    "Entrega local",
    "Serviço ambulante",
    "Feira artesanal",
    "Aula particular",
)


def _storage():
    module = importlib.import_module("documents.storage_s3")
    return importlib.reload(module)


def _default_tenant() -> str:
    return os.getenv("DEFAULT_TENANT_ID") or _storage().DEFAULT_TENANT


def _random_cnpj(rng: random.Random) -> str:
    return "".join(rng.choices(string.digits, k=14))


def _random_date(rng: random.Random) -> date:
    today = date.today()
    return today - timedelta(days=rng.randint(1, 365))


def _random_amount(rng: random.Random) -> float:
    return round(rng.uniform(35, 950), 2)


def _inject_noise(rng: random.Random, text: str, ratio: float = 0.04) -> str:
    tokens = list(text)
    injections = max(1, int(len(tokens) * ratio))
    noisy = tokens.copy()
    for _ in range(injections):
        idx = rng.randrange(len(noisy))
        noisy.insert(idx, rng.choice(["*", "#", "  ", " "]))
    return "".join(noisy)


def _apply_intentional_error(
    rng: random.Random, amount: float, cnpj: str, issued: date
) -> tuple[float, str, date, str]:
    fault = rng.choice(["amount", "date", "cnpj"])
    if fault == "amount":
        amount = round(amount * rng.uniform(1.12, 1.3), 2)
    elif fault == "date":
        issued = issued + timedelta(days=rng.randint(15, 90))
    else:
        cnpj = f"{cnpj[:-3]}999"  # troca final para simular erro humano
    return amount, cnpj, issued, fault


def _build_text(doc_id: str, kind: str, amount: float, cnpj: str, issued: date, service: str) -> str:
    header = "NFSe PREFEITURA" if kind == "NFSe" else "RECIBO DE PAGAMENTO MEI"
    return "\n".join(
        [
            header,
            f"Documento: {doc_id}",
            f"Prestador CNPJ: {cnpj}",
            f"Serviço: {service}",
            f"Data Emissão: {issued.isoformat()}",
            f"Valor Total: R$ {amount:.2f}",
            "Autenticidade: " + uuid.uuid4().hex[:16].upper(),
        ]
    )


def generate_synthetic_documents(
    count: int = 50,
    error_ratio: float = 0.1,
    seed: int | None = None,
    tenant_id: str | None = None,
) -> List[SyntheticDoc]:
    rng = random.Random(seed)
    docs: List[SyntheticDoc] = []
    num_errors = min(count, max(1, int(count * error_ratio)))
    error_slots = set(rng.sample(range(count), num_errors))
    resolved_tenant = tenant_id or os.getenv("DEFAULT_TENANT_ID") or _storage().DEFAULT_TENANT

    for idx in range(count):
        doc_id = uuid.uuid4().hex
        kind = rng.choice(["NFSe", "RECIBO"])
        amount = _random_amount(rng)
        cnpj = _random_cnpj(rng)
        issued = _random_date(rng)
        service = rng.choice(_SERVICES)

        has_error = idx in error_slots
        fault_hint = ""
        if has_error:
            amount, cnpj, issued, fault_hint = _apply_intentional_error(rng, amount, cnpj, issued)

        text = _build_text(doc_id, kind, amount, cnpj, issued, service)
        noisy_text = _inject_noise(rng, text)
        if has_error:
            noisy_text += f"\nObservacao: campo intencionalmente alterado ({fault_hint})"

        docs.append(
            SyntheticDoc(
                doc_id=doc_id,
                tenant_id=resolved_tenant,
                kind=kind,
                text=noisy_text,
                has_error=has_error,
            )
        )

    return docs


def upload_documents_via_presign(
    documents: Iterable[SyntheticDoc], tenant_id: str | None = None
) -> list[dict]:
    results: list[dict] = []
    storage = _storage()
    for doc in documents:
        target_tenant = tenant_id or doc.tenant_id
        key = f"synthetic/{doc.doc_id}.txt"
        presigned = storage.presign_put(key, "text/plain", target_tenant)
        storage.upload_via_presign(presigned, doc.text.encode("utf-8"), content_type="text/plain")
        results.append(
            {
                "doc_id": doc.doc_id,
                "tenant_id": target_tenant,
                "key": key,
                "storage_key": presigned["key"],
                "has_error": doc.has_error,
            }
        )
    return results


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Gerar e subir documentos MEI sintéticos")
    parser.add_argument("--count", type=int, default=50, help="Quantidade de documentos (padrão: 50)")
    parser.add_argument("--tenant", type=str, default=_default_tenant(), help="Tenant alvo")
    parser.add_argument("--error-rate", type=float, default=0.1, help="Percentual de erros propositais")
    parser.add_argument("--seed", type=int, default=None, help="Seed opcional para reprodutibilidade")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    docs = generate_synthetic_documents(
        count=args.count,
        error_ratio=args.error_rate,
        seed=args.seed,
        tenant_id=args.tenant,
    )
    uploads = upload_documents_via_presign(docs, tenant_id=args.tenant)
    intentional_errors = sum(1 for item in uploads if item["has_error"])
    print(
        f"Gerados {len(docs)} documentos sintéticos para o tenant '{args.tenant}' "
        f"(erros intencionais em {intentional_errors})."
    )


if __name__ == "__main__":
    main()
