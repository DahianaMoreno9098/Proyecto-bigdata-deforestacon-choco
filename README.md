# proyecto-bigdata-deforestacion-choco

Equipo de trabajo:
Dahiana Andrea Moreno Palomeque

Proyecto Final · Módulo Big Data · Docente: Yeis Livis Taborda Henao · Periodo I-2026

---

# 1. CASO DEL NEGOCIO

**Descripción del Problema:** El departamento del Chocó, parte de la región biogeográfica
del Pacífico colombiano, presenta una pérdida sostenida de cobertura boscosa. El conjunto de
datos analizado registra **7.930 eventos de deforestación entre 2014 y 2021**, distribuidos
en 31 municipios, equivalentes a **~63.963 hectáreas** de bosque perdido. Hoy, la trazabilidad
de estos eventos depende de procesos manuales y fragmentados entre CODECHOCÓ (autoridad
ambiental regional) e IDEAM (alertas satelitales nacionales): las coordenadas se capturan como
texto en formato Grados-Minutos-Segundos, las áreas usan separador decimal de coma, y
consolidar un reporte puede tardar semanas, tiempo en el que un frente minero o de cultivo
puede duplicar su extensión.

El histórico además revela un sesgo técnico relevante: en 2016 el número de eventos
detectados cae de forma abrupta (227 eventos, frente a más de 1.400 en 2014) no porque la
deforestación se redujera, sino por la alta nubosidad en las imágenes satelitales ópticas
disponibles ese año — exactamente el tipo de patrón que un proceso manual difícilmente
detecta, pero que una plataforma de Big Data con linaje de datos puede señalar de forma
sistemática.

**1.1 Objetivo general**
Diseñar e implementar una solución de Big Data en Databricks que permita monitorear, limpiar
y analizar a escala los registros de deforestación del Chocó, para anticipar focos críticos y
priorizar la capacidad de control ambiental de CODECHOCÓ.

**1.2 Objetivos específicos**

- Construir un pipeline automatizado (Bronze → Silver → Gold) que estandarice coordenadas,
  áreas y nombres de municipio dispersos en un único origen de verdad confiable.
- Clasificar municipios por nivel de alerta (Moderada / Media / Alta / Extrema) según
  hectáreas deforestadas, para priorizar operativos de campo.
- Entrenar un modelo predictivo que estime la probabilidad de que un municipio entre en
  alerta alta/extrema, a partir de su histórico de causas y eventos.

Esta solución permite pasar de un enfoque reactivo (reportes manuales después del hecho) a uno
predictivo y trazable en la gestión ambiental del departamento.

---

# 2. RELACIÓN BENEFICIO/COSTE

La automatización del pipeline reduce el tiempo de consolidación de reportes de **3-4 semanas
a menos de 24 horas**, liberando aproximadamente 288 horas/mes de trabajo analítico repetitivo
(conversión manual de coordenadas, limpieza de nombres de municipio, hojas de cálculo), un
ahorro estimado de **$86.400.000 COP/año** en horas-hombre.

Frente a una inversión inicial estimada de **$88.600.000 COP** (plataforma cloud, desarrollo
de pipelines, licencias y capacitación) y un costo operativo recurrente de
**$55.000.000 COP/año**, el proyecto genera un beneficio neto recurrente de
**~$31.400.000 COP/año** solo por eficiencia operativa — con un periodo de recuperación de la
inversión cercano a **2,8 años**, sin contar los beneficios ambientales adicionales.

El verdadero valor adicional está en habilitar un sistema de Medición, Reporte y Verificación
(MRV) trazable, requisito para acceder a mecanismos de financiamiento climático y mercados de
carbono (de forma análoga a esquemas como Visión Amazonía / REDD+ en Colombia), además de
reducir costos de restauración ecológica al intervenir focos de deforestación de forma
temprana en lugar de reactiva.

*Nota: estas cifras son estimaciones con fines de modelado financiero académico; deben
validarse con cotizaciones reales antes de una implementación en producción.*

---

# 3. ARQUITECTURA PROPUESTA

## Arquitectura del Proyecto

![Arquitectura](images/Arquitectura.png)

CSV (CODECHOCÓ / IDEAM) → Unity Catalog Volume → Tabla Bronze → Tabla Silver → Modelo de
Machine Learning → Tabla Gold → Lakeflow Job → Tablero de monitoreo

**Fuente de Datos:** registros geoespaciales de eventos de deforestación en el Chocó
(2014–2021): ID, tipo de geometría, año, causa, área en hectáreas, observación de calidad,
coordenadas y municipio.

**Almacenamiento (Volumes):** el archivo CSV se carga en un Volume de Unity Catalog, que
actúa como zona de aterrizaje (landing zone) gobernada.

**Bronze – Datos crudos:** se carga el archivo en una tabla Delta sin ninguna transformación,
para conservar la información original y permitir trazabilidad completa.

**Silver – Transformación y enriquecimiento:** se aplican procesos de calidad:

- Conversión de coordenadas de Grados-Minutos-Segundos (texto) a grados decimales (WGS84)
- Conversión de `AREA_Ha` de texto con coma decimal a tipo numérico
- Estandarización de nombres de municipio
- Marca de calidad para el sesgo de nubosidad satelital de 2016
- Eliminación de duplicados y validación de rangos lógicos

**Machine Learning – Scoring de riesgo:** se entrena un modelo de Random Forest para estimar
la probabilidad de que una combinación municipio-año entre en nivel de alerta alto o extremo.

Resultado generado: `nivel_alerta` / `alerta_alta` (probabilidad)

**Gold – Datos para negocio:** se crea la tabla final `gold_hotspots_deforestacion`, con
hectáreas y eventos totales por municipio-año, causa predominante y nivel de alerta. Esta
tabla está lista para consumo analítico.

**Lakeflow Jobs:** automatización y orquestación del pipeline completo.

## Job Automatizado

![Job](images/JOB.png)
*Ejecución real del pipeline como Lakeflow Job (Succeeded, 42s) en Databricks Free Edition.*

**Visualización:** la tabla Gold alimenta el tablero de monitoreo, que permite identificar
municipios de alto riesgo, analizar la distribución por causa y priorizar operativos de campo.
La arquitectura combina Big Data, Machine Learning y Business Intelligence en un flujo
completo, de principio a fin.

---

# 4. PIPELINE DE INGESTA DE DATOS

## Databricks

![Databricks](images/Databricks.png)
*Notebook ejecutándose en Databricks Free Edition: carga real de los 7.930 registros en la capa Bronze.*

La información se obtiene desde un archivo CSV cargado en un Volume de Unity Catalog y es
procesada mediante el notebook
[`Pipeline_BigData_Deforestacion_Choco.ipynb`](notebooks/Pipeline_BigData_Deforestacion_Choco.ipynb),
que ejecuta cada una de las etapas del flujo de datos.

**Automatización de la ingesta:** la carga de datos se realiza mediante un notebook en
Databricks que lee, procesa y almacena la información en tablas Delta Lake. Este proceso se
integra con **Lakeflow Jobs**, permitiendo programar ejecuciones automáticas sin intervención
manual.

**Estrategia Medallion:**

| Capa | Contenido | Tabla generada |
|---|---|---|
| Bronze | Carga cruda del CSV, sin transformaciones | `bronze_deforestacion_choco` |
| Silver | Limpieza, conversión de coordenadas y tipos, estandarización, deduplicación | `silver_deforestacion_choco_limpio` |
| Gold | Agregados por municipio-año, causa predominante y nivel de alerta | `gold_hotspots_deforestacion` |

**Pipelines y Workflows:** el pipeline completo se automatiza mediante **Lakeflow Jobs**,
permitiendo procesar la información de forma continua desde su origen hasta la generación de
valor para el negocio.

# Lineage

## Lineage en Databricks (Unity Catalog)

![Lineage](images/Lineage.png)
*Grafo de linaje real de Unity Catalog: bronze_deforestacion_choco → silver_deforestacion_choco_limpio → gold_hotspots_deforestacion, con trazabilidad a nivel de columna.*

Se utiliza la funcionalidad de **Lineage Graph** de Unity Catalog para validar la trazabilidad
completa del dato: qué notebook transformó cada tabla y de qué tabla anterior proviene. El
linaje garantiza gobernanza, auditoría y control del ciclo de vida del dato dentro de la
arquitectura Medallion.

---

# 5. MODELOS DE CIENCIA DE DATOS

**Análisis descriptivo:** sobre 7.930 eventos georreferenciados (2014–2021) en 31 municipios,
con un total acumulado de 63.963 hectáreas deforestadas. La distribución por evento es
fuertemente asimétrica (promedio 8,07 Ha, máximo 6.988,9 Ha), señal de pocos eventos extremos
—probablemente frentes mineros— que concentran una proporción desproporcionada del área
total.

![Eventos por año](images/chart_anio.png)
*Caída de eventos en 2016 por sesgo técnico de nubosidad satelital, no por reducción real.*

![Top municipios](images/chart_municipios.png)
*Riosucio y Unguía concentran, juntos, más del 55% del área deforestada acumulada.*

![Causas](images/chart_causas.png)
*Incendios y minería explican el 70% del área deforestada, aunque el cultivo es la causa más frecuente en número de eventos.*

**Modelado:** se entrenó un modelo de **Random Forest** sobre la tabla Gold (189 combinaciones
municipio-año) para estimar la probabilidad de que una combinación caiga en alerta alta o
extrema, usando como variables la causa predominante y el número de eventos históricos.

El dataset se dividió 75% entrenamiento / 25% prueba. Resultado obtenido al ejecutar el
notebook en Databricks Free Edition: **AUC-ROC = 0.834**, **F1-score = 0.760**.

El experimento se registra en **MLflow** (parámetros, métricas y el modelo serializado),
permitiendo comparar versiones y justificar ante CODECHOCÓ por qué se eligió un modelo
específico, en lugar de presentarlo como una caja negra.

---

# 6. APP O VISUALIZACIÓN

## Tablero de Monitoreo (Mockup)

![Mockup del tablero](images/dashboard_mockup.png)

*Este es un **mockup ilustrativo (boceto)** de la estructura del tablero final, no una
captura de una plataforma real. Representa cómo se vería el tablero de monitoreo construido
sobre la tabla `gold_hotspots_deforestacion`: tarjetas KPI, ranking de municipios, distribución
por causa, semáforo de alerta regional, tendencia anual y tabla de alertas recientes.*

**Serving Endpoint:** la tabla Gold se expone mediante un **Databricks SQL Warehouse**, que
actúa como punto único de consumo tanto para herramientas de BI como para integraciones vía
API con aplicaciones de campo. El acceso se controla mediante permisos de Unity Catalog.

El modelo entrenado queda registrado en MLflow como un servicio de analítica que permite
generar predicciones de forma periódica y disponibilizarlas para consumo por herramientas de
negocio.

---

# Cómo ejecutar este proyecto

1. Crea una cuenta gratuita en **Databricks Free Edition**: https://www.databricks.com/try-databricks
   (no requiere tarjeta de crédito; reemplazó a la antigua Community Edition, retirada el 1 de
   enero de 2026).
2. En **Catalog Explorer**, crea un Volume y sube el archivo
   [`data/AREAS_DEFORESTADAS_CHOCO_20260511.csv`](data/AREAS_DEFORESTADAS_CHOCO_20260511.csv).
3. Importa el notebook
   [`notebooks/Pipeline_BigData_Deforestacion_Choco.ipynb`](notebooks/Pipeline_BigData_Deforestacion_Choco.ipynb)
   a tu workspace (Workspace → Import).
4. Ajusta las variables `catalog`, `schema` y `volume_path` en la primera celda de código para
   que coincidan con tu workspace.
5. Ejecuta el notebook completo (Run All).
6. Toma tus propias capturas y reemplaza los archivos `images/JOB.png`, `images/Lineage.png`
   y `images/Databricks.png` siguiendo las instrucciones de la última celda del notebook.

## Contenido del repositorio

```
proyecto-bigdata-deforestacion-choco/
├── README.md
├── data/
│   └── AREAS_DEFORESTADAS_CHOCO_20260511.csv
├── notebooks/
│   └── Pipeline_BigData_Deforestacion_Choco.ipynb
├── images/
│   ├── Arquitectura.png
│   ├── JOB.png            (placeholder — reemplazar con captura real)
│   ├── Lineage.png        (placeholder — reemplazar con captura real)
│   ├── Databricks.png     (placeholder — reemplazar con captura real)
│   ├── chart_anio.png
│   ├── chart_municipios.png
│   ├── chart_causas.png
│   └── dashboard_mockup.png
└── docs/
    └── PROYECTO_FINAL_BIGDATA_DEFORESTACION_CHOCO.docx
```
