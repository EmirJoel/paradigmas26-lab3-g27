# Variables
SBT = sbt
ARGS = --subscription-file data/valid_subscriptions.json --entities-dir data/valid_entities --top-k 10

.PHONY: all compile run clean

# El objetivo por defecto compila y ejecuta el programa
all: compile run

compile:
	@echo "Compilando el proyecto con sbt..."
	$(SBT) compile

run:
	@echo "Ejecutando RedditNER distribuido..."
	$(SBT) "run $(ARGS)"

clean:
	@echo "Limpiando archivos temporales y compilados..."
	$(SBT) clean