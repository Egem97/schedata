# 🚨 Solución de Problemas Docker

## Problema Actual: Error de Docker Compose v1

### Síntomas:
- `ModuleNotFoundError: No module named 'distutils'`
- Errores con `docker-compose==1.29.2`
- Problemas de importación de módulos Python

### Causa:
- Docker Compose v1 (instalado via pip) es incompatible con Python 3.12+
- El módulo `distutils` fue removido en Python 3.12
- Versión antigua de Docker Compose

---

## 🔧 Solución Rápida

### **Opción 1: Usar script de solución automática**
```bash
# Dar permisos
chmod +x fix_docker.sh

# Ejecutar solución
./fix_docker.sh

# Reiniciar terminal
source ~/.bashrc
```

### **Opción 2: Usar Docker Compose v2**
```bash
# Usar script alternativo
chmod +x deploy-v2.sh

# Desplegar con v2
./deploy-v2.sh deploy
```

---

## 🛠️ Solución Manual

### **1. Desinstalar Docker Compose v1**
```bash
# Desinstalar via pip
sudo pip uninstall docker-compose
sudo pip3 uninstall docker-compose

# Remover binarios
sudo rm -f /usr/local/bin/docker-compose
sudo rm -f /usr/bin/docker-compose
```

### **2. Instalar Docker Compose v2**

#### Ubuntu/Debian:
```bash
# Actualizar repositorios
sudo apt update

# Instalar plugin oficial
sudo apt install docker-compose-plugin

# Verificar instalación
docker compose version
```

#### CentOS/RHEL:
```bash
# Instalar plugin
sudo yum install docker-compose-plugin

# Verificar
docker compose version
```

#### Instalación manual:
```bash
# Descargar última versión
DOCKER_COMPOSE_VERSION=$(curl -s https://api.github.com/repos/docker/compose/releases/latest | grep -oP '"tag_name": "\K(.*)(?=")')

# Descargar e instalar
sudo curl -L "https://github.com/docker/compose/releases/download/${DOCKER_COMPOSE_VERSION}/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose

# Dar permisos
sudo chmod +x /usr/local/bin/docker-compose
```

### **3. Verificar instalación**
```bash
# Verificar Docker Compose v2
docker compose version

# Si no funciona, verificar Docker
docker --version
```

---

## 📋 Comandos Actualizados

### **Comandos nuevos (v2):**
```bash
# En lugar de docker-compose, usar:
docker compose up -d
docker compose down
docker compose logs -f
docker compose ps
docker compose build
```

### **Usar script actualizado:**
```bash
# Script que detecta automáticamente la versión
chmod +x deploy-v2.sh

# Comandos disponibles
./deploy-v2.sh deploy    # Desplegar
./deploy-v2.sh status    # Estado
./deploy-v2.sh logs      # Logs
./deploy-v2.sh restart   # Reiniciar
./deploy-v2.sh stop      # Detener
```

---

## 🔍 Verificación de Problemas

### **Verificar versión de Python:**
```bash
python3 --version
# Si es 3.12+, usar Dockerfile.simple
```

### **Verificar Docker:**
```bash
docker --version
docker info
```

### **Verificar Docker Compose:**
```bash
# Probar v2
docker compose version

# Probar v1
docker-compose --version
```

---

## 🚀 Despliegue Alternativo

### **Usar Dockerfile simplificado:**
```bash
# Renombrar Dockerfile
mv Dockerfile Dockerfile.original
mv Dockerfile.simple Dockerfile

# Usar archivo compose v2
./deploy-v2.sh deploy
```

### **Usar Docker directamente:**
```bash
# Construir imagen
docker build -t batch-alza .

# Ejecutar contenedor
docker run -d \
  --name batch-alza-scheduler \
  --restart unless-stopped \
  -v $(pwd)/config.yaml:/app/config.yaml:ro \
  -v $(pwd)/logs:/app/logs \
  batch-alza
```

---

## 🐛 Problemas Específicos

### **Error: `distutils` no encontrado**
```bash
# Solución: Usar Python 3.11 específico
# Editar Dockerfile primera línea:
FROM python:3.11.9-slim-bullseye
```

### **Error: `docker-compose` no encontrado**
```bash
# Solución: Usar docker compose v2
docker compose version

# Si no funciona, instalar plugin
sudo apt install docker-compose-plugin
```

### **Error: Permisos denegados**
```bash
# Agregar usuario al grupo docker
sudo usermod -aG docker $USER

# Reiniciar sesión
exit
# Volver a conectar por SSH
```

### **Error: No se puede conectar a Docker**
```bash
# Iniciar Docker
sudo systemctl start docker
sudo systemctl enable docker

# Verificar estado
sudo systemctl status docker
```

---

## ✅ Verificación Final

### **Probar configuración:**
```bash
# Verificar Docker
docker run hello-world

# Verificar Compose
docker compose version

# Verificar archivos
ls -la config.yaml requirements.txt scheduler.py

# Probar despliegue
./deploy-v2.sh deploy
```

### **Ver logs:**
```bash
# Logs en tiempo real
./deploy-v2.sh logs

# Logs específicos
docker logs batch-alza-scheduler
```

---

## 🆘 Si Nada Funciona

### **Reinstalar Docker completo:**
```bash
# Desinstalar todo
sudo apt remove docker docker-engine docker.io containerd runc docker-compose

# Reinstalar Docker
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh

# Instalar Compose v2
sudo apt install docker-compose-plugin

# Verificar
docker --version
docker compose version
```

### **Usar contenedor base diferente:**
```bash
# Cambiar primera línea del Dockerfile
FROM python:3.11.9-slim-bullseye
# o
FROM python:3.10-slim
```

---

## 📞 Contacto

Si sigues teniendo problemas:
1. Ejecuta `./fix_docker.sh` 
2. Usa `./deploy-v2.sh deploy`
3. Comparte los logs con `./deploy-v2.sh logs`

## 🎯 Resumen de Solución

1. **Problema**: Docker Compose v1 incompatible
2. **Solución**: Usar Docker Compose v2 
3. **Comando**: `./deploy-v2.sh deploy`
4. **Verificación**: `docker compose version` 

# 🛠️ Guía de Solución de Problemas

Esta guía te ayudará a resolver los problemas más comunes al desplegar el sistema automatizado.

## 🚨 Error: EOFError: EOF when reading a line

### Síntomas:
```
EOFError: EOF when reading a line
File "/app/scheduler.py", line 244, in main
respuesta = input().lower().strip()
```

### Causa:
El scheduler intenta leer entrada del usuario pero Docker está en modo no interactivo.

### Solución:
El scheduler ahora detecta automáticamente si está en modo no interactivo y usa configuración del `config.yaml`:

```yaml
scheduler:
  interval: "minutes"
  minutes: 15
  ejecutar_inicial: false  # Controla si ejecuta proceso al inicio en Docker
```

### Opciones:
- `ejecutar_inicial: false` - No ejecuta proceso al inicio (recomendado)
- `ejecutar_inicial: true` - Ejecuta proceso una vez al inicio

## 🐳 Docker no requiere sudo

### Problema:
Necesitas usar `sudo` antes de cada comando Docker:
```bash
sudo docker ps
sudo docker-compose up
```

### Solución paso a paso:

1. **Agregar usuario al grupo docker:**
```bash
sudo usermod -aG docker $USER
```

2. **Aplicar cambios:**
```bash
# Opción A: Cerrar sesión y volver a conectar
exit
# Vuelve a conectarte por SSH

# Opción B: Usar newgrp (sin cerrar sesión)
newgrp docker

# Opción C: Reiniciar Docker
sudo systemctl restart docker
```

3. **Verificar:**
```bash
docker --version
docker ps
groups $USER  # Deberías ver 'docker' en la lista
```

### Actualizar scripts después:
Una vez configurado, puedes quitar `sudo` de los scripts:
- `deploy.sh`
- `deploy-v2.sh`
- `fix_docker.sh`

## 📋 Otros problemas comunes

### 1. **Problema con Python 3.12+ y distutils**

**Error:**
```
ModuleNotFoundError: No module named 'distutils'
```

**Solución:**
```bash
# Usar Dockerfile.simple con Python 3.11
docker build -f Dockerfile.simple -t batch-alza-scheduler .
```

### 2. **Docker Compose v1 vs v2**

**Síntomas:**
- Comandos `docker-compose` no funcionan
- Errores de sintaxis en docker-compose.yml

**Solución:**
```bash
# Ejecutar script de actualización
bash fix_docker.sh

# O usar versión específica
bash deploy-v2.sh deploy
```

### 3. **Problemas de permisos en logs**

**Error:**
```
PermissionError: [Errno 13] Permission denied: './logs/scheduler.log'
```

**Solución:**
```bash
# Crear directorio de logs con permisos
mkdir -p logs
chmod 755 logs

# O en Docker
docker exec -it batch-alza-scheduler mkdir -p /app/logs
```

### 4. **Archivo config.yaml no encontrado**

**Error:**
```
FileNotFoundError: [Errno 2] No such file or directory: 'config.yaml'
```

**Solución:**
```bash
# Copiar y editar configuración
cp config.yaml.example config.yaml
# Editar con tus credenciales reales
nano config.yaml
```

### 5. **Problemas de red en Docker**

**Error:**
```
requests.exceptions.ConnectionError: HTTPSConnectionPool
```

**Solución:**
```bash
# Verificar conectividad
docker exec batch-alza-scheduler ping -c 3 google.com

# Reiniciar contenedor
docker restart batch-alza-scheduler
```

### 6. **Contenedor se detiene inmediatamente**

**Debugging:**
```bash
# Ver logs del contenedor
docker logs batch-alza-scheduler

# Ejecutar en modo debug
docker run -it --rm batch-alza-scheduler python -c "import sys; print(sys.version)"
```

### 7. **Credenciales inválidas**

**Error:**
```
400 Bad Request: Invalid client credentials
```

**Solución:**
1. Verificar `config.yaml` tiene credenciales correctas
2. Verificar permisos en Azure Portal
3. Regenerar client_secret si es necesario

### 8. **Problemas de timezone**

**Error:**
Horarios incorrectos en logs

**Solución:**
```yaml
# En docker-compose.yml
environment:
  - TZ=America/Lima  # Cambiar por tu zona horaria
```

### 9. **Memoria insuficiente**

**Error:**
```
MemoryError: Unable to allocate array
```

**Solución:**
```bash
# Aumentar memoria del contenedor
docker run -m 2g batch-alza-scheduler
```

### 10. **Archivos no encontrados en OneDrive**

**Error:**
```
❌ No se encontró el archivo: archivo.xlsx
```

**Solución:**
1. Verificar nombres exactos en `config.yaml`
2. Verificar IDs de carpetas
3. Verificar permisos del token

## 🔧 Comandos útiles para debugging

```bash
# Ver estado de servicios
docker ps -a
docker-compose ps

# Ver logs en tiempo real
docker logs -f batch-alza-scheduler

# Ejecutar shell en contenedor
docker exec -it batch-alza-scheduler /bin/bash

# Verificar configuración
docker exec batch-alza-scheduler python -c "from utils.get_token import print_config; print_config()"

# Reiniciar completamente
docker-compose down
docker-compose up -d

# Limpiar todo (cuidado!)
docker-compose down -v
docker system prune -a
```

## 📞 Obtener ayuda

Si los problemas persisten:

1. **Revisar logs detallados:**
```bash
docker logs batch-alza-scheduler > debug.log 2>&1
```

2. **Verificar configuración:**
```bash
docker exec batch-alza-scheduler python -c "from utils.get_token import get_config_value; print(get_config_value('scheduler', 'interval'))"
```

3. **Probar conectividad:**
```bash
docker exec batch-alza-scheduler python -c "import requests; print(requests.get('https://httpbin.org/ip').json())"
```

4. **Verificar dependencias:**
```bash
docker exec batch-alza-scheduler python -c "import yaml, pandas, schedule; print('OK')"
```

Recuerda siempre verificar que tu `config.yaml` tiene las credenciales correctas y que los permisos en Azure están configurados apropiadamente. 