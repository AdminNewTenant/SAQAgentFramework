import os
import asyncio

from agent_framework.azure import AzureOpenAIChatClient
from agent_framework import MCPStreamableHTTPTool

from dotenv import load_dotenv

import logging
from opentelemetry import trace
from opentelemetry.trace import set_tracer_provider
from opentelemetry._logs import set_logger_provider
# from opentelemetry.exporter.otlp.proto.grpc._log_exporter import Con
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor, ConsoleLogExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter

# Service name is required for most backends
resource = Resource.create(attributes={
    "service.name": "Demonstration"
})


def setup_logging():
    # Create and set a global logger provider for the application.
    logger_provider = LoggerProvider(resource=resource)
    # Log processors are initialized with an exporter which is responsible
    logger_provider.add_log_record_processor(BatchLogRecordProcessor(ConsoleLogExporter()))
    # Sets the global default logger provider
    set_logger_provider(logger_provider)
    # Create a logging handler to write logging records, in OTLP format, to the exporter.
    handler = LoggingHandler()
    # Attach the handler to the root logger. `getLogger()` with no arguments returns the root logger.
    # Events from all child loggers will be processed by this handler.
    logger = logging.getLogger()
    logger.addHandler(handler)
    # Set the logging level to NOTSET to allow all records to be processed by the handler.
    logger.setLevel(logging.NOTSET)


def setup_tracing():
    # Initialize a trace provider for the application. This is a factory for creating tracers.
    tracer_provider = TracerProvider(resource=resource)
    # Span processors are initialized with an exporter which is responsible
    # for sending the telemetry data to a particular backend.
    tracer_provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
    # Sets the global default tracer provider
    set_tracer_provider(tracer_provider)

# setup_logging()
# setup_tracing()

load_dotenv()   

api_key = os.environ['SUBSCRIPTION_API_KEY']
mcp_server_url = os.environ['MCP_SERVER']

INSTRUCTIONS = """🥂 Agent de Contenu d'Inspiration – SAQ

L'Agent de Contenu d'Inspiration de la SAQ est un assistant intelligent conçu pour enrichir l’expérience des amateurs de cocktails. Il agit comme un guide conversationnel, capable de répondre aux questions des usagers sur les recettes, les ingrédients, les techniques de préparation et les accords avec les produits disponibles à la SAQ.

🎯 Fonctionnalités principales

Recherche de cocktails : Permet aux usagers de trouver des recettes selon leurs préférences (type d’alcool, saveur, occasion, niveau de difficulté). Les cocktails sont exposés via l'outil read_cocktails.

Suggestions personnalisées : Propose des cocktails en fonction des produits disponibles en succursale via l'outil read_stocks_in_shops.Réponses aux questions : Fournit des explications claires sur les étapes de préparation, les outils nécessaires, et les variantes possibles.

Découverte de produits : Met en valeur les spiritueux, liqueurs et autres ingrédients vendus à la SAQ, en les intégrant aux recettes.

🧠 Comportement attendu

Être convivial, informatif et accessible à tous les niveaux de connaissance. Utiliser un ton chaleureux et professionnel, fidèle à l’image de la SAQ. Encourager la découverte et la curiosité autour de l’univers des cocktails. Respecter les normes de consommation responsable."""

async def main() -> None:
    print("=== Azure Chat Client with Explicit Settings ===")

    saq_tools = MCPStreamableHTTPTool(
            name="Société des Alcools du Québec Tools",
            url=mcp_server_url,
            headers={"Ocp-Apim-Subscription-Key": f"{api_key}"})

    agent = AzureOpenAIChatClient(
        deployment_name="gpt-4o",
        endpoint=f"https://saq-apim-basic.azure-api.net/azureopenai/",
        api_key = api_key,
    ).create_agent(
        instructions=INSTRUCTIONS,
        api_version="2024-05-01-preview",
        tools=[saq_tools],
    )

    result = await agent.run("""Could you provide me with 3 cocktail suggestions for the month of october? 
                             Please provide one cocktail for the following 3 alcools: 'Rhum blanc', 'Triple sec', 'Tequila'""")
    print(f"Result: {result}\n")

if __name__ == "__main__":
    asyncio.run(main())
