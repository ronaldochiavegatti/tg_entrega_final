from __future__ import annotations

import difflib
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import List, Optional

import reflex as rx


@dataclass
class LimitItem:
    name: str
    state: str
    accumulated: float
    forecast: float
    capacity: float
    estimated_date: Optional[date]

    @property
    def utilization(self) -> float:
        if self.capacity <= 0:
            return 0
        return min((self.accumulated / self.capacity) * 100, 100)


@dataclass
class DocumentItem:
    id: int
    title: str
    category: str
    extracted: str
    corrected: str
    confidence: float

    def apply_patch(self, patch: str) -> None:
        self.corrected = patch


@dataclass
class ChatMessage:
    sender: str
    content: str
    citations: Optional[List[str]] = None


class AppState(rx.State):
    """Centralized app state for pages and components."""

    # Dashboard state
    limits: List[LimitItem] = [
        LimitItem(
            name="Limite de Crédito",
            state="Saudável",
            accumulated=32000,
            forecast=38000,
            capacity=50000,
            estimated_date=datetime.today().date() + timedelta(days=30),
        ),
        LimitItem(
            name="Limite de Risco",
            state="Atenção",
            accumulated=42000,
            forecast=52000,
            capacity=60000,
            estimated_date=datetime.today().date() + timedelta(days=20),
        ),
        LimitItem(
            name="Limite Operacional",
            state="Crítico",
            accumulated=58000,
            forecast=70000,
            capacity=70000,
            estimated_date=datetime.today().date() + timedelta(days=10),
        ),
    ]
    last_recalculated: Optional[datetime] = None

    # Document screen
    documents: List[DocumentItem] = [
        DocumentItem(
            id=1,
            title="Contrato #2031",
            category="Contrato",
            extracted="O cliente solicitou valor de R$ 10.000,00 com vencimento em 30 dias.",
            corrected="Solicitação de R$ 10.000,00 com vencimento em 30 dias.",
            confidence=0.92,
        ),
        DocumentItem(
            id=2,
            title="Fatura #991",
            category="Financeiro",
            extracted="Fatura apresenta diferença de R$ 150,00 no cálculo de impostos.",
            corrected="Ajuste de R$ 150,00 identificado nos impostos da fatura.",
            confidence=0.88,
        ),
        DocumentItem(
            id=3,
            title="Laudo Técnico",
            category="Operação",
            extracted="Equipamento operando acima da capacidade nominal há 3 dias.",
            corrected="Equipamento excede capacidade nominal há 3 dias.",
            confidence=0.81,
        ),
    ]
    search_query: str = ""
    filter_category: str = "Todos"
    selected_doc_id: Optional[int] = 1
    selected_ids: set[int] = set()
    patch_text: str = ""
    bulk_patch: str = ""

    # Login state
    username: str = ""
    password: str = ""
    is_authenticated: bool = False
    welcome_message: str = ""

    # Chat state
    chat_history: List[ChatMessage] = [
        ChatMessage(
            sender="assistente",
            content="Olá! Posso ajudar a corrigir campos, mostrar limites ou recalcular limites.",
            citations=["limites.md#overview"],
        )
    ]
    user_input: str = ""

    # Export state
    last_export: str = ""

    def refresh_limits(self):
        """Refresh limit information, simulating a recalculation."""
        now = datetime.now()
        for limit in self.limits:
            limit.accumulated = round(limit.accumulated * 1.01, 2)
            limit.forecast = round(limit.forecast * 1.02, 2)
            if limit.accumulated >= limit.capacity:
                limit.state = "Crítico"
            elif limit.forecast >= limit.capacity * 0.9:
                limit.state = "Atenção"
            else:
                limit.state = "Saudável"
            days_until = max(int((limit.capacity - limit.forecast) / 1500), 1)
            limit.estimated_date = now.date() + timedelta(days=days_until)
        self.last_recalculated = now

    def set_username(self, value: str):
        self.username = value

    def set_password(self, value: str):
        self.password = value

    def login(self):
        if not self.username or not self.password:
            rx.toast.error("Informe usuário e senha.")
            return
        self.is_authenticated = True
        self.welcome_message = f"Bem-vindo, {self.username}!"
        rx.toast.success("Login realizado com sucesso.")

    def on_limits_recalculated(self, _: Optional[dict] = None):
        self.refresh_limits()
        rx.toast.success("Limites recalculados automaticamente.")

    @rx.var
    def limit_summary(self) -> List[dict]:
        return [
            {
                "name": limit.name,
                "state": limit.state,
                "accumulated": f"R$ {limit.accumulated:,.2f}",
                "forecast": f"R$ {limit.forecast:,.2f}",
                "capacity": f"R$ {limit.capacity:,.2f}",
                "estimated_date": limit.estimated_date.strftime("%d/%m/%Y")
                if limit.estimated_date
                else "-",
                "progress": limit.utilization,
            }
            for limit in self.limits
        ]

    # Document helpers
    @rx.var
    def categories(self) -> List[str]:
        base = sorted({doc.category for doc in self.documents})
        return ["Todos", *base]

    @rx.var
    def filtered_documents(self) -> List[DocumentItem]:
        docs = self.documents
        if self.filter_category != "Todos":
            docs = [doc for doc in docs if doc.category == self.filter_category]
        if self.search_query:
            query = self.search_query.lower()
            docs = [
                doc
                for doc in docs
                if query in doc.title.lower()
                or query in doc.extracted.lower()
                or query in doc.corrected.lower()
            ]
        return docs

    @rx.var
    def selected_document(self) -> Optional[DocumentItem]:
        if self.selected_doc_id is None and self.filtered_documents:
            return self.filtered_documents[0]
        for doc in self.filtered_documents:
            if doc.id == self.selected_doc_id:
                return doc
        return None

    @rx.var
    def diff_preview(self) -> str:
        doc = self.selected_document
        if not doc:
            return ""
        diff = difflib.ndiff(doc.extracted.split(), doc.corrected.split())
        return " ".join(diff)

    def set_search_query(self, value: str):
        self.search_query = value

    def set_filter_category(self, value: str):
        self.filter_category = value

    def select_document(self, doc_id: int):
        self.selected_doc_id = doc_id
        self.patch_text = self.selected_document.corrected if self.selected_document else ""

    def set_patch_text(self, value: str):
        self.patch_text = value

    def toggle_select(self, doc_id: int):
        if doc_id in self.selected_ids:
            self.selected_ids.remove(doc_id)
        else:
            self.selected_ids.add(doc_id)

    def select_all_filtered(self):
        self.selected_ids = {doc.id for doc in self.filtered_documents}

    def apply_patch(self):
        if not self.selected_document:
            rx.toast.error("Selecione um documento para aplicar o PATCH.")
            return
        self.selected_document.apply_patch(self.patch_text)
        rx.toast.success("PATCH aplicado ao documento.")

    def apply_bulk_patch(self):
        if not self.bulk_patch:
            rx.toast.error("Nenhum PATCH informado para aplicar em lote.")
            return
        targets = [doc for doc in self.documents if doc.id in self.selected_ids]
        if not targets:
            rx.toast.error("Selecione documentos para bulk edit.")
            return
        for doc in targets:
            doc.apply_patch(self.bulk_patch)
        rx.toast.success(f"PATCH aplicado em {len(targets)} documentos.")

    def set_bulk_patch(self, value: str):
        self.bulk_patch = value

    def send_message(self):
        text = self.user_input.strip()
        if not text:
            return
        self.chat_history.append(ChatMessage(sender="usuário", content=text))
        self.user_input = ""
        self.chat_history.append(
            ChatMessage(
                sender="assistente",
                content="Ajustei conforme sua mensagem e gerei um resumo com citações.",
                citations=["doc_regras.md#patch", "limites.md#forecast"],
            )
        )

    def set_user_input(self, value: str):
        self.user_input = value

    def send_quick_action(self, action: str):
        friendly = {
            "corrigir_campo": "Corrigir campo",
            "mostrar_limites": "Mostrar limites",
            "recalcular_limites": "Recalcular limites",
        }.get(action, action)
        self.chat_history.append(ChatMessage(sender="usuário", content=friendly))
        if action == "recalcular_limites":
            self.refresh_limits()
            bot_response = "Limites recalculados. Atualizei o painel e registrei o novo forecast."
        elif action == "mostrar_limites":
            bot_response = "Aqui estão os limites atualizados com previsão e estado."
        else:
            bot_response = "Corrigi os campos indicados e gerei o PATCH sugerido."
        self.chat_history.append(
            ChatMessage(
                sender="assistente",
                content=bot_response,
                citations=["limites.md#visao-geral", "docs.md#corrigir"]
                if action != "corrigir_campo"
                else ["docs.md#patch-inline"],
            )
        )

    def export_csv(self):
        targets = self.selected_ids or {doc.id for doc in self.filtered_documents}
        if not targets:
            rx.toast.error("Nenhum documento disponível para exportar.")
            return
        self.last_export = f"CSV exportado para {len(targets)} documentos"
        rx.toast.success(self.last_export)

    def export_pdf(self):
        targets = self.selected_ids or {doc.id for doc in self.filtered_documents}
        if not targets:
            rx.toast.error("Nenhum documento disponível para exportar.")
            return
        self.last_export = f"PDF exportado para {len(targets)} documentos"
        rx.toast.success(self.last_export)

