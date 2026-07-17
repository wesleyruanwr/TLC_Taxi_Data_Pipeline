"""
Script que substitui a celula de conexao hardcoded do notebook analise_ny_taxi.ipynb
por uma versao que usa python-dotenv para ler as credenciais do .env.
Execute uma unica vez: python patch_notebook.py
"""
import json
import os

NOTEBOOK_PATH = os.path.join(os.path.dirname(__file__), 
    "..", "notebooks", "analise_ny_taxi.ipynb")

NEW_CELL_SOURCE = [
    "import os\n",
    "import pandas as pd\n",
    "import sqlalchemy\n",
    "import matplotlib.pyplot as plt\n",
    "import matplotlib.ticker as mticker\n",
    "import seaborn as sns\n",
    "import warnings\n",
    "from dotenv import load_dotenv\n",
    "warnings.filterwarnings('ignore')\n",
    "\n",
    "# carrega as variaveis do .env na raiz do projeto\n",
    "dotenv_path = os.path.join(os.path.dirname(os.getcwd()), '.env')\n",
    "if not os.path.exists(dotenv_path):\n",
    "    dotenv_path = os.path.join(os.getcwd(), '.env')  # fallback se rodar da raiz\n",
    "load_dotenv(dotenv_path)\n",
    "\n",
    "# configs visuais\n",
    "sns.set_theme(style='darkgrid')\n",
    "plt.rcParams['figure.figsize'] = (14, 5)\n",
    "plt.rcParams['font.size'] = 12\n",
    "\n",
    "# conexao com o banco — credenciais lidas do .env, sem hardcode\n",
    "db_user     = os.environ.get('POSTGRES_USER', 'postgres')\n",
    "db_password = os.environ.get('POSTGRES_PASSWORD', 'postgres')\n",
    "db_name     = os.environ.get('NY_TAXI_DB_NAME', 'ny_taxi')\n",
    "engine = sqlalchemy.create_engine(f'postgresql://{db_user}:{db_password}@localhost:5432/{db_name}')\n",
    "print('conexao com o banco estabelecida com sucesso')"
]

with open(NOTEBOOK_PATH, "r", encoding="utf-8") as f:
    nb = json.load(f)

patched = False
for cell in nb["cells"]:
    if cell.get("cell_type") == "code":
        src = "".join(cell.get("source", []))
        # identifica a celula de conexao pelo trecho caracteristico
        if "create_engine" in src and "localhost:5432" in src:
            cell["source"] = NEW_CELL_SOURCE
            patched = True
            print("Celula de conexao atualizada com sucesso.")
            break

if not patched:
    print("AVISO: celula de conexao nao encontrada. Verifique o notebook manualmente.")

with open(NOTEBOOK_PATH, "w", encoding="utf-8") as f:
    json.dump(nb, f, ensure_ascii=False, indent=1)

print("Notebook salvo.")
