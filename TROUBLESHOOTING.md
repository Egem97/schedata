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