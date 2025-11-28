import reflex as rx

from .state import AppState, ChatMessage, DocumentItem


NAV_LINKS = [
    ("Login", "/login"),
    ("Dashboard", "/dashboard"),
    ("Documentos", "/documents"),
    ("Chat", "/chat"),
]


def navbar() -> rx.Component:
    return rx.hstack(
        rx.text("TG Limites", font_weight="bold", font_size="1.1rem"),
        rx.spacer(),
        *[
            rx.link(
                label,
                href=href,
                padding_x="12px",
                font_weight="medium",
                underline="none",
            )
            for label, href in NAV_LINKS
        ],
        padding="16px",
        border_bottom="1px solid #e5e7eb",
        background_color="white",
    )


def page_shell(title: str, *children: rx.Component) -> rx.Component:
    return rx.box(
        navbar(),
        rx.vstack(
            rx.heading(title, size="lg"),
            *children,
            spacing="16px",
            padding="24px",
            align_items="stretch",
        ),
        background_color="#f9fafb",
        min_height="100vh",
    )


def limit_card(limit: dict) -> rx.Component:
    status_color = {
        "Saudável": "green",
        "Atenção": "yellow",
        "Crítico": "red",
    }.get(limit["state"], "gray")
    return rx.box(
        rx.hstack(
            rx.heading(limit["name"], size="md"),
            rx.badge(limit["state"], color_scheme=status_color),
            rx.spacer(),
        ),
        rx.text(f"Acumulado: {limit['accumulated']}", font_size="0.95rem"),
        rx.text(f"Forecast: {limit['forecast']}", font_size="0.95rem"),
        rx.text(f"Capacidade: {limit['capacity']}", font_size="0.95rem"),
        rx.text(f"Estimativa: {limit['estimated_date']}", font_size="0.9rem", color="gray"),
        rx.progress(value=limit["progress"], color_scheme=status_color),
        border="1px solid #e5e7eb",
        border_radius="12px",
        padding="16px",
        background_color="white",
        box_shadow="sm",
        width="100%",
    )


def document_row(doc: DocumentItem) -> rx.Component:
    return rx.hstack(
        rx.checkbox(
            is_checked=lambda doc_id=doc.id: doc_id in AppState.selected_ids,
            on_change=lambda _: AppState.toggle_select(doc.id),
        ),
        rx.vstack(
            rx.hstack(
                rx.text(doc.title, font_weight="medium"),
                rx.badge(doc.category),
                rx.spacer(),
                rx.text(f"Confiança {doc.confidence:.0%}", color="gray"),
            ),
            rx.text(doc.corrected, color="gray"),
        ),
        rx.button(
            "Detalhar",
            size="sm",
            on_click=lambda: AppState.select_document(doc.id),
            variant="outline",
        ),
        border_bottom="1px solid #e5e7eb",
        padding_y="12px",
        width="100%",
    )


def document_grid() -> rx.Component:
    return rx.box(
        rx.hstack(
            rx.input(
                placeholder="Buscar documentos...",
                value=AppState.search_query,
                on_change=AppState.set_search_query,
                width="100%",
            ),
            rx.select(
                AppState.categories,
                placeholder="Filtrar categoria",
                value=AppState.filter_category,
                on_change=AppState.set_filter_category,
                width="200px",
            ),
            spacing="12px",
            width="100%",
        ),
        rx.button(
            "Selecionar todos",
            size="sm",
            align_self="flex-end",
            on_click=AppState.select_all_filtered,
            variant="soft",
        ),
        rx.vstack(
            rx.foreach(AppState.filtered_documents, document_row),
            spacing="0px",
            width="100%",
            background_color="white",
            border="1px solid #e5e7eb",
            border_radius="12px",
            padding_x="12px",
        ),
        spacing="12px",
        width="100%",
    )


def document_detail() -> rx.Component:
    return rx.box(
        rx.heading("Detalhe", size="md"),
        rx.cond(
            AppState.selected_document is None,
            rx.text("Selecione um documento para ver detalhes."),
            rx.vstack(
                rx.text(lambda: AppState.selected_document.title, font_weight="bold"),
                rx.hstack(
                    rx.text("Confidence:"),
                    rx.badge(lambda: f"{AppState.selected_document.confidence:.0%}"),
                ),
                rx.text("Extraído"),
                rx.code(lambda: AppState.selected_document.extracted, white_space="normal"),
                rx.text("Corrigido"),
                rx.code(lambda: AppState.selected_document.corrected, white_space="normal"),
                rx.text("Diff extraído vs corrigido"),
                rx.code(AppState.diff_preview, white_space="normal"),
                rx.text_area(
                    placeholder="Editar PATCH inline...",
                    value=AppState.patch_text,
                    on_change=AppState.set_patch_text,
                    min_height="120px",
                ),
                rx.button("Aplicar PATCH", on_click=AppState.apply_patch),
                spacing="8px",
                align_items="stretch",
            ),
        ),
        background_color="white",
        border="1px solid #e5e7eb",
        border_radius="12px",
        padding="16px",
        width="100%",
    )


def bulk_edit_panel() -> rx.Component:
    return rx.box(
        rx.heading("Bulk edit", size="sm"),
        rx.text("Aplique um PATCH em massa aos documentos selecionados."),
        rx.text_area(
            placeholder="PATCH para aplicar em lote",
            value=AppState.bulk_patch,
            on_change=AppState.set_bulk_patch,
            min_height="80px",
        ),
        rx.button("Aplicar em lote", on_click=AppState.apply_bulk_patch),
        background_color="white",
        border="1px solid #e5e7eb",
        border_radius="12px",
        padding="16px",
        width="100%",
    )


def export_buttons() -> rx.Component:
    return rx.hstack(
        rx.button("Exportar CSV", on_click=AppState.export_csv, color_scheme="blue"),
        rx.button("Exportar PDF", on_click=AppState.export_pdf, variant="outline"),
        rx.cond(
            AppState.last_export != "",
            rx.badge(lambda: AppState.last_export, color_scheme="green"),
        ),
        spacing="12px",
    )


def chat_message(message: ChatMessage) -> rx.Component:
    alignment = "flex-end" if message.sender == "usuário" else "flex-start"
    bubble_color = "#e0f2fe" if message.sender == "assistente" else "#f1f5f9"
    return rx.vstack(
        rx.text(message.sender.title(), font_size="0.85rem", color="gray"),
        rx.box(
            rx.text(message.content),
            rx.cond(
                message.citations,
                rx.hstack(
                    rx.text("Citações:"),
                    rx.foreach(
                        message.citations,
                        lambda cit: rx.badge(cit, variant="surface", color_scheme="purple"),
                    ),
                    spacing="8px",
                    flex_wrap="wrap",
                ),
            ),
            background_color=bubble_color,
            padding="10px",
            border_radius="10px",
            width="100%",
        ),
        align_items=alignment,
        width="100%",
    )


def chat_panel() -> rx.Component:
    return rx.vstack(
        rx.vstack(
            rx.foreach(AppState.chat_history, chat_message),
            spacing="12px",
            height="400px",
            overflow_y="auto",
            padding="12px",
            background_color="white",
            border="1px solid #e5e7eb",
            border_radius="12px",
            width="100%",
        ),
        rx.hstack(
            rx.button(
                "Corrigir campo",
                size="sm",
                on_click=lambda: AppState.send_quick_action("corrigir_campo"),
            ),
            rx.button(
                "Mostrar limites",
                size="sm",
                on_click=lambda: AppState.send_quick_action("mostrar_limites"),
            ),
            rx.button(
                "Recalcular limites",
                size="sm",
                on_click=lambda: AppState.send_quick_action("recalcular_limites"),
            ),
            spacing="8px",
            wrap="wrap",
        ),
        rx.hstack(
            rx.input(
                placeholder="Digite sua mensagem...",
                value=AppState.user_input,
                on_change=AppState.set_user_input,
                width="100%",
            ),
            rx.button("Enviar", on_click=AppState.send_message),
            spacing="8px",
            width="100%",
        ),
        spacing="12px",
        width="100%",
    )

