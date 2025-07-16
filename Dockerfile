# Usar Python 3.11 como base
FROM python:3.11-slim

# Información del contenedor
LABEL maintainer="Tu Nombre <tu@email.com>"
LABEL description="Sistema automatizado de procesamiento de datos con schedule"

# Establecer el directorio de trabajo
WORKDIR /app

# Instalar dependencias del sistema
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copiar archivo de dependencias
COPY requirements.txt .

# Instalar dependencias de Python
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código fuente
COPY . .

# Crear directorio para logs
RUN mkdir -p /app/logs

# Crear usuario no-root para seguridad
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

# Exponer puerto (opcional, por si necesitas monitoring)
EXPOSE 8800

# Comando por defecto
CMD ["python", "scheduler.py"] 