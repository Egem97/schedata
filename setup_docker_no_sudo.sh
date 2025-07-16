#!/bin/bash

# Script para configurar Docker sin sudo
# Autor: Sistema Automatizado

set -e

echo "🐳 Configurando Docker para funcionar sin sudo..."
echo "================================================"

# Verificar si el usuario ya está en el grupo docker
if groups $USER | grep -q '\bdocker\b'; then
    echo "✅ El usuario $USER ya está en el grupo docker"
else
    echo "📝 Agregando usuario $USER al grupo docker..."
    sudo usermod -aG docker $USER
    echo "✅ Usuario agregado al grupo docker"
fi

# Verificar que el grupo docker existe
if getent group docker > /dev/null 2>&1; then
    echo "✅ El grupo docker existe"
else
    echo "📝 Creando grupo docker..."
    sudo groupadd docker
    echo "✅ Grupo docker creado"
fi

# Verificar el socket de Docker
echo "🔍 Verificando permisos del socket de Docker..."
if [ -e /var/run/docker.sock ]; then
    echo "✅ Socket de Docker encontrado: /var/run/docker.sock"
    ls -la /var/run/docker.sock
else
    echo "❌ Socket de Docker no encontrado"
    exit 1
fi

# Reiniciar el servicio Docker
echo "🔄 Reiniciando servicio Docker..."
sudo systemctl restart docker

echo ""
echo "🎉 Configuración completada!"
echo "================================================"
echo ""
echo "⚠️  IMPORTANTE: Para que los cambios surtan efecto, necesitas:"
echo ""
echo "Opción 1 (Recomendada): Cerrar sesión y volver a conectar"
echo "   exit"
echo "   # Vuelve a conectarte por SSH"
echo ""
echo "Opción 2: Usar newgrp (sin cerrar sesión)"
echo "   newgrp docker"
echo ""
echo "Opción 3: Reiniciar el sistema"
echo "   sudo reboot"
echo ""
echo "Después de aplicar los cambios, prueba:"
echo "   docker --version"
echo "   docker ps"
echo ""
echo "Si funciona sin sudo, puedes actualizar los scripts:"
echo "   - deploy.sh"
echo "   - deploy-v2.sh"
echo "   - fix_docker.sh"
echo ""
echo "Para quitar todos los 'sudo docker' por 'docker'"
echo "================================================" 