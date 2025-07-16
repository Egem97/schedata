# 🐳 Guía de Despliegue con Docker en VPS

Esta guía te ayudará a desplegar el sistema automatizado de procesamiento de datos en tu VPS usando Docker.

## 📋 Requisitos Previos

### En tu VPS:
- **Docker** instalado
- **Docker Compose** instalado
- **Git** instalado
- Acceso SSH al VPS

### En tu máquina local:
- Archivo `config.yaml` con tus credenciales reales

## 🚀 Instalación de Docker en VPS

### Ubuntu/Debian:
```bash
# Actualizar sistema
sudo apt update && sudo apt upgrade -y

# Instalar Docker
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh

# Agregar usuario al grupo docker
sudo usermod -aG docker $USER

# Instalar Docker Compose
sudo apt install docker-compose -y

# Verificar instalación
docker --version
docker-compose --version
```

### CentOS/RHEL:
```bash
# Instalar Docker
sudo yum install -y docker
sudo systemctl start docker
sudo systemctl enable docker

# Instalar Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/download/v2.24.0/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
```

## 📂 Despliegue Paso a Paso

### 1. **Clonar el repositorio en el VPS**
```bash
# Conectar al VPS
ssh usuario@tu-vps-ip

# Clonar el proyecto
git clone https://github.com/tu-usuario/tu-repositorio.git
cd tu-repositorio
```

### 2. **Configurar credenciales**
```bash
# Crear archivo de configuración
cp config.yaml.example config.yaml

# Editar con tus credenciales reales
nano config.yaml
```

**Importante**: Completa todas las secciones con tus credenciales reales:
```yaml
microsoft_graph:
  tenant_id: "tu_tenant_id_real"
  client_id: "tu_client_id_real"
  client_secret: "tu_client_secret_real"

onedrive:
  drive_id: "tu_drive_id_real"
  
google_sheets:
  spreadsheet_id: "tu_spreadsheet_id_real"
```

### 3. **Dar permisos al script de despliegue**
```bash
chmod +x deploy.sh
```

### 4. **Desplegar el proyecto**
```bash
# Despliegue completo
./deploy.sh deploy
```

Este comando:
- ✅ Verifica que Docker esté instalado
- ✅ Verifica que todos los archivos necesarios existan
- ✅ Crea directorios necesarios
- ✅ Construye la imagen Docker
- ✅ Inicia el contenedor
- ✅ Muestra el estado final

## 🎛️ Comandos de Administración

### Comandos principales:
```bash
# Desplegar todo
./deploy.sh deploy

# Ver estado del contenedor
./deploy.sh status

# Ver logs en tiempo real
./deploy.sh logs

# Reiniciar contenedor
./deploy.sh restart

# Detener contenedor
./deploy.sh stop

# Iniciar contenedor
./deploy.sh start

# Limpiar todo (contenedores e imágenes)
./deploy.sh cleanup
```

### Comandos Docker directos:
```bash
# Ver contenedores corriendo
docker ps

# Ver logs del contenedor
docker logs batch-alza-scheduler

# Entrar al contenedor
docker exec -it batch-alza-scheduler /bin/bash

# Ver uso de recursos
docker stats batch-alza-scheduler
```

## 📊 Monitoreo

### Ver logs en tiempo real:
```bash
./deploy.sh logs
```

### Ver estado del contenedor:
```bash
./deploy.sh status
```

### Ver logs del sistema:
```bash
# Logs del contenedor
docker logs batch-alza-scheduler

# Logs persistentes en el host
tail -f logs/scheduler.log
```

## 🔧 Configuración Avanzada

### Cambiar zona horaria:
Edita `docker-compose.yml`:
```yaml
environment:
  - TZ=America/Lima  # Cambiar por tu zona horaria
```

### Configurar recursos:
Agrega límites de recursos en `docker-compose.yml`:
```yaml
deploy:
  resources:
    limits:
      memory: 512M
      cpus: '0.5'
```

### Configurar reinicio automático:
```yaml
restart: unless-stopped  # Ya configurado por defecto
```

## 🔄 Actualizaciones

### Para actualizar el código:
```bash
# Detener contenedor
./deploy.sh stop

# Actualizar código
git pull origin main

# Reconstruir y reiniciar
./deploy.sh deploy
```

### Para actualizar solo configuración:
```bash
# Editar configuración
nano config.yaml

# Reiniciar contenedor
./deploy.sh restart
```

## 🛡️ Seguridad

### Firewall (UFW):
```bash
# Permitir SSH
sudo ufw allow ssh

# Permitir Docker (si necesitas acceso externo)
sudo ufw allow 2376/tcp

# Activar firewall
sudo ufw enable
```

### Backup de logs:
```bash
# Crear backup de logs
tar -czf logs_backup_$(date +%Y%m%d).tar.gz logs/

# Automatizar backup (agregar a crontab)
0 2 * * * tar -czf /backup/logs_backup_$(date +\%Y\%m\%d).tar.gz /ruta/proyecto/logs/
```

## 🚨 Solución de Problemas

### Si el contenedor no inicia:
```bash
# Ver logs de error
docker logs batch-alza-scheduler

# Verificar configuración
./deploy.sh status
```

### Si hay problemas de memoria:
```bash
# Ver uso de recursos
docker stats

# Limpiar imágenes no utilizadas
docker system prune -a
```

### Si hay problemas de permisos:
```bash
# Verificar permisos de archivos
ls -la config.yaml
ls -la logs/

# Corregir permisos si es necesario
sudo chown -R $USER:$USER .
```

## 🔄 Automatización con Systemd

### Crear servicio systemd:
```bash
sudo nano /etc/systemd/system/batch-alza.service
```

Contenido:
```ini
[Unit]
Description=Batch Alza Scheduler
After=docker.service
Requires=docker.service

[Service]
Type=oneshot
RemainAfterExit=yes
WorkingDirectory=/ruta/a/tu/proyecto
ExecStart=/ruta/a/tu/proyecto/deploy.sh start
ExecStop=/ruta/a/tu/proyecto/deploy.sh stop
User=tu-usuario

[Install]
WantedBy=multi-user.target
```

### Activar servicio:
```bash
sudo systemctl daemon-reload
sudo systemctl enable batch-alza.service
sudo systemctl start batch-alza.service
```

## 📈 Monitoreo Avanzado

### Healthcheck:
El contenedor incluye un healthcheck automático que verifica cada 30 segundos si el proceso está funcionando.

### Alertas por email (opcional):
Puedes configurar alertas cuando el contenedor se reinicie agregando scripts de notificación.

## 🎯 Comandos Útiles

```bash
# Ver todas las imágenes
docker images

# Limpiar todo Docker
docker system prune -a --volumes

# Backup del volumen de datos
docker run --rm -v batch-alza_scheduler_data:/data -v $(pwd):/backup alpine tar czf /backup/data_backup.tar.gz /data

# Restaurar backup
docker run --rm -v batch-alza_scheduler_data:/data -v $(pwd):/backup alpine tar xzf /backup/data_backup.tar.gz -C /
```

---

## 🎉 ¡Listo!

Tu sistema automatizado ahora está corriendo en Docker en tu VPS. El scheduler se ejecutará según la configuración en `config.yaml` y los logs estarán disponibles tanto en el contenedor como en el directorio `logs/` del host.

Para cualquier problema, revisa los logs con `./deploy.sh logs` o contacta al administrador del sistema. 