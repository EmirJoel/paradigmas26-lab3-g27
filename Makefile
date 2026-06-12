# Variables
SBT = sbt
ARGS = --subscription-file data/valid_subscriptions.json --entities-dir data/valid_entities --top-k 10
LOCAL_ARGS = --subscription-file data/local_subscriptions.json --entities-dir data/valid_entities --top-k 10

.PHONY: all compile run local test clean

# El objetivo por defecto compila y ejecuta con las suscripciones reales
all: compile run

compile:
	@echo "Compilando el proyecto con sbt..."
	$(SBT) compile

run:
	@echo "Ejecutando RedditNER distribuido (Modo Real)..."
	$(SBT) "run $(ARGS)"

local:
	@echo "Ejecutando RedditNER distribuido (Modo Local con Mock Server)..."
	$(SBT) "run $(LOCAL_ARGS)"

test:
	@echo "Ejecutando suite de tests de integracion..."
	bash tests.sh

clean:
	@echo "Limpiando archivos temporales y compilados..."
	$(SBT) clean