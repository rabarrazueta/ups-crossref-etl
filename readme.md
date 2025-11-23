```markdown
# Pipeline ETL para Análisis Bibliográfico - CrossRef API

Pipeline automatizado de extracción, transformación y carga (ETL) de publicaciones científicas desde la API de CrossRef, con integración a Apache Superset para visualización y análisis de la producción académica de la Universidad Politécnica Salesiana (UPS).

![Python](https://img.shields.io/badge/Python-3.11%2B-blue)
![SQLite](https://img.shields.io/badge/SQLite-3-green)
![License](https://img.shields.io/badge/License-MIT-yellow)

## 📋 Descripción

Este proyecto implementa un sistema completo de análisis bibliométrico que:
- Extrae datos de publicaciones científicas desde CrossRef API
- Transforma y normaliza información de autores, afiliaciones e instituciones
- Carga los datos en una base de datos SQLite relacional normalizada
- Integra con Apache Superset para dashboards interactivos.

IMPORTANTE:
Este proyecto fue realizado como parte de un reto técnico.
El uso del nombre de la institución es únicamente para fines ilustrativos y el acceso a los datos es completamente público.

## 🚀 Características

- **ETL Automatizado**: Extracción con paginación, reintentos y manejo de errores
- **Normalización de Datos**: Limpieza de afiliaciones, nombres de autores y metadatos
- **Base de Datos Relacional**: Esquema normalizado con tablas relacionadas
- **Auditoría**: Logs detallados de cada ejecución del pipeline
- **Escalable**: Diseñado para procesar grandes volúmenes de publicaciones
- **Visualización**: Dashboards con filtros interactivos en Apache Superset

## 🛠️ Tecnologías

- Python 3.11+
- SQLite3
- Pandas
- Requests
- Apache Superset
- CrossRef REST API

## 📁 Estructura del Proyecto

ups-crossref-etl/
├── src/
│ └── barrazueta_pipeline_etl_crossref.py
├── docs/
│ └── Documentacion-Tecnica-Robinson-Barrazueta.pdf
├── data/
│ └── ups_institucional.csv
├── images/
│ └── dashboard_screenshot.png
├── README.md
├── .gitignore
└── requirements.txt

text

## ⚙️ Instalación

### Requisitos Previos

- Python 3.11 o superior
- pip (gestor de paquetes de Python)

### Pasos

**1. Clona este repositorio:**

git clone https://github.com/TU-USUARIO/ups-crossref-etl.git
cd ups-crossref-etl

text

**2. Crea un entorno virtual:**

python -m venv venv

text

**3. Activa el entorno virtual:**

- En Windows:

venv\Scripts\activate

text

- En Linux/Mac:

source venv/bin/activate

text

**4. Instala las dependencias:**

pip install -r requirements.txt

text

## 🎯 Uso

**1. Configura tu email en el script** (línea 12 de `src/barrazueta_pipeline_etl_crossref.py`):

MAILTO = "tu-email@ejemplo.com"

text

**2. Ejecuta el pipeline:**

python src/barrazueta_pipeline_etl_crossref.py

text

**3. Resultado:**

La base de datos se creará automáticamente como `barrazueta_db_ups_crossref.db`

**4. Visualización:**

Conecta Apache Superset a la base de datos SQLite para visualizar los resultados.

## 📊 Base de Datos

El esquema incluye las siguientes tablas:

- **obras**: Publicaciones científicas con metadatos completos
- **autores**: Información de autores únicos
- **afiliaciones**: Instituciones y afiliaciones académicas
- **obras_autores**: Relación many-to-many entre obras y autores
- **autores_afiliaciones**: Relación entre autores y sus afiliaciones
- **ejecuciones**: Log de auditoría del pipeline

## 📄 Documentación

Consulta la documentación técnica completa en [`docs/Documentacion-Tecnica-Robinson-Barrazueta.pdf`](docs/Documentacion-Tecnica-Robinson-Barrazueta.pdf)

## 👨‍💻 Autor

**Robinson Barrazueta**

- Email: rabarrazueta@utpl.edu.ec
- Universidad Técnica Particular de Loja (UTPL)
- Universidad Politécnica Salesiana (UPS)

## 📝 Licencia

Este proyecto está bajo la Licencia MIT.

## 🙏 Agradecimientos

- CrossRef por proporcionar acceso gratuito a su API
- Apache Superset por la plataforma de visualización
