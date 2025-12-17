# Usa el runtime base de Astronomer (Airflow 2.x, Python 3.12).
FROM astrocrpublic.azurecr.io/runtime:3.1-7

# --- 1. CONFIGURACIÓN DEL USUARIO ---
# El usuario predeterminado en Astro Runtime es 'astro'.
USER astro

# --------------------------------------------------------------------------
# --- 2. INSTALACIÓN DE DEPENDENCIAS DE PYTHON (ALTA CACHÉ Y VELOCIDAD) ---
# Paso CRÍTICO para la velocidad: Copia SOLO el archivo de dependencias de runtime.
COPY requirements.txt .

# Ejecuta la instalación.
RUN pip install --no-cache-dir -r requirements.txt

# --------------------------------------------------------------------------

# --- 3. COPIA E INSTALACIÓN EDITABLE DEL CÓDIGO (BAJA CACHÉ) ---

# Paso de Copia: Mantiene la propiedad 'astro:astro'.
COPY --chown=astro:astro . .

# --------------------------------------------------------------------------
# --- CORRECCIÓN CRÍTICA DE PERMISOS ---
# Si la instalación falla por permisos, forzamos la propiedad del directorio
# de trabajo al usuario 'astro' usando 'root'.
# Esto garantiza que el usuario 'astro' pueda escribir los metadatos de instalación.
USER root
RUN chown -R astro:astro /usr/local/airflow
USER astro
# --------------------------------------------------------------------------

# Instala el proyecto como un paquete editable.
RUN pip install -e .
