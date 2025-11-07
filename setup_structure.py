import os

# Struttura di cartelle e file da creare
structure = {
    "eubot_analytics": {
        "main.py": "",
        "requirements.txt": "",
        "config": {
            "settings.py": "",
            "registry.yaml": "",
            "prompts": {
                "router_prompt.txt": "",
            },
        },
        "modules": {
            "telegram_bot.py": "",
            "llm_router.py": "",
            "fetchers": {
                "ecb_adapter.py": "",
            },
            "plotter.py": "",
        },
        "data": {},
    }
}


def create_structure(base_path, tree):
    """Crea ricorsivamente cartelle e file."""
    for name, content in tree.items():
        path = os.path.join(base_path, name)
        if isinstance(content, dict):  # è una cartella
            os.makedirs(path, exist_ok=True)
            create_structure(path, content)
        else:  # è un file
            with open(path, "w", encoding="utf-8") as f:
                f.write(content)
            print(f"✅ creato: {path}")


if __name__ == "__main__":
    root = os.getcwd()  # directory corrente
    create_structure(root, structure)
    print("\n🎉 Struttura progetto 'eubot_analytics' creata con successo!")
