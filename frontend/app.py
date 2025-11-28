import reflex as rx

from . import components
from .state import AppState


max_width = "1200px"


def login_page() -> rx.Component:
    return components.page_shell(
        "Login",
        rx.center(
            rx.vstack(
                rx.heading("Acesse sua conta", size="md"),
                rx.input(
                    placeholder="Usuário",
                    value=AppState.username,
                    on_change=AppState.set_username,
                    width="100%",
                ),
                rx.input(
                    placeholder="Senha",
                    type="password",
                    value=AppState.password,
                    on_change=AppState.set_password,
                    width="100%",
                ),
                rx.button("Entrar", width="100%", on_click=AppState.login),
                rx.cond(
                    AppState.is_authenticated,
                    rx.badge(lambda: AppState.welcome_message, color_scheme="green"),
                ),
                spacing="12px",
                width="360px",
                background_color="white",
                padding="24px",
                border_radius="12px",
                border="1px solid #e5e7eb",
                box_shadow="md",
            ),
            width="100%",
        ),
    )


def dashboard_page() -> rx.Component:
    return components.page_shell(
        "Dashboard de Limites",
        rx.text(
            "Painel com estado atual, acumulado, forecast, barra de progresso e data estimada.",
            color="gray",
        ),
        rx.grid(
            rx.foreach(AppState.limit_summary, components.limit_card),
            min_child_width="320px",
            spacing="12px",
            width="100%",
            max_width=max_width,
        ),
        rx.card(
            rx.hstack(
                rx.text("Último recálculo:"),
                rx.text(lambda: AppState.last_recalculated.strftime("%d/%m/%Y %H:%M:%S")
                        if AppState.last_recalculated else "-"),
            ),
            rx.text("O painel é atualizado automaticamente ao receber LIMITS_RECALCULATED."),
            rx.button("Recalcular agora", on_click=AppState.refresh_limits),
            rx.socket_event("LIMITS_RECALCULATED", AppState.on_limits_recalculated),
            width="100%",
            max_width=max_width,
        ),
    )


def documents_page() -> rx.Component:
    return components.page_shell(
        "Documentos",
        rx.text("Grid de documentos com busca, filtros, diff e edição inline."),
        rx.vstack(
            components.document_grid(),
            rx.grid(
                components.document_detail(),
                components.bulk_edit_panel(),
                columns=2,
                spacing="12px",
                width="100%",
                max_width=max_width,
            ),
            components.export_buttons(),
            spacing="16px",
            width="100%",
            max_width=max_width,
        ),
    )


def chat_page() -> rx.Component:
    return components.page_shell(
        "Chat",
        rx.text("Chat com ações rápidas e citações RAG nas respostas do assistente."),
        components.chat_panel(),
        max_width=max_width,
    )


app = rx.App(state=AppState)
app.add_page(login_page, route="/login", title="Login")
app.add_page(dashboard_page, route="/dashboard", title="Dashboard")
app.add_page(documents_page, route="/documents", title="Documentos")
app.add_page(chat_page, route="/chat", title="Chat")
app.compile()
