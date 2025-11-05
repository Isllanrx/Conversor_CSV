import sys
from pathlib import Path

if __name__ == "__main__":
    # Adicionar o diretório atual ao sys.path para garantir imports corretos
    diretorio_atual = Path(__file__).parent.absolute()
    if str(diretorio_atual) not in sys.path:
        sys.path.insert(0, str(diretorio_atual))

    try:
        from interface import AplicacaoConversor
    except ModuleNotFoundError as erro:
        if "customtkinter" in str(erro):
            print("ERRO: Dependências não instaladas!")
            print("\nPor favor, execute um dos seguintes comandos:")
            print("  1. PowerShell: .\\run.ps1")
            print("  2. CMD: run.bat")
            print("  3. Ou instale manualmente: pip install -r requirements.txt")
            print("\nSe estiver usando ambiente virtual, certifique-se de ativá-lo primeiro.")
            sys.exit(1)
        raise

    app: AplicacaoConversor = AplicacaoConversor()
    app.mainloop()
else:
    from .interface import AplicacaoConversor
