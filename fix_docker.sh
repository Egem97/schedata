#!/bin/bash

# Script para solucionar problemas de Docker Compose
# Soluciona errores de módulos Python y actualiza Docker Compose

set -e

# Colores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Función para detectar el sistema operativo
detect_os() {
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        if [ -f /etc/debian_version ]; then
            echo "debian"
        elif [ -f /etc/redhat-release ]; then
            echo "redhat"
        else
            echo "linux"
        fi
    elif [[ "$OSTYPE" == "darwin"* ]]; then
        echo "macos"
    else
        echo "unknown"
    fi
}

# Función para desinstalar Docker Compose v1
remove_old_compose() {
    log_info "Desinstalando Docker Compose v1..."
    
    # Desinstalar pip version
    sudo pip uninstall docker-compose -y 2>/dev/null || true
    sudo pip3 uninstall docker-compose -y 2>/dev/null || true
    
    # Remover binario si existe
    sudo rm -f /usr/local/bin/docker-compose
    sudo rm -f /usr/bin/docker-compose
    
    log_success "Docker Compose v1 desinstalado"
}

# Función para instalar Docker Compose v2
install_compose_v2() {
    log_info "Instalando Docker Compose v2..."
    
    local os=$(detect_os)
    
    if [[ "$os" == "debian" ]]; then
        # Ubuntu/Debian
        sudo apt update
        sudo apt install -y docker-compose-plugin
        
    elif [[ "$os" == "redhat" ]]; then
        # CentOS/RHEL
        sudo yum install -y docker-compose-plugin
        
    else
        # Instalación manual
        DOCKER_COMPOSE_VERSION=$(curl -s https://api.github.com/repos/docker/compose/releases/latest | grep -oP '"tag_name": "\K(.*)(?=")')
        sudo curl -L "https://github.com/docker/compose/releases/download/${DOCKER_COMPOSE_VERSION}/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
        sudo chmod +x /usr/local/bin/docker-compose
    fi
    
    log_success "Docker Compose v2 instalado"
}

# Función para verificar Python y instalar dependencias
fix_python_deps() {
    log_info "Verificando dependencias de Python..."
    
    # Verificar versión de Python
    python_version=$(python3 --version 2>&1 | awk '{print $2}' | cut -d. -f1,2)
    log_info "Versión de Python: $python_version"
    
    # Instalar distutils si es necesario (Python 3.12+)
    if [[ "$python_version" > "3.11" ]]; then
        log_warning "Python 3.12+ detectado, instalando setuptools..."
        sudo apt update
        sudo apt install -y python3-setuptools python3-distutils || true
        pip3 install setuptools --upgrade || true
    fi
    
    # Instalar dependencias adicionales
    sudo apt install -y python3-pip python3-dev build-essential || true
    
    log_success "Dependencias de Python verificadas"
}

# Función para actualizar Docker si es necesario
update_docker() {
    log_info "Verificando Docker..."
    
    if ! command -v docker &> /dev/null; then
        log_error "Docker no está instalado"
        exit 1
    fi
    
    # Actualizar Docker si es una versión muy antigua
    docker_version=$(docker --version | awk '{print $3}' | sed 's/,//')
    log_info "Versión de Docker: $docker_version"
    
    log_success "Docker verificado"
}

# Función para crear alias para docker-compose
create_compose_alias() {
    log_info "Creando alias para docker-compose..."
    
    # Verificar si docker compose (v2) está disponible
    if docker compose version &> /dev/null; then
        echo 'alias docker-compose="docker compose"' >> ~/.bashrc
        echo 'alias docker-compose="docker compose"' >> ~/.zshrc 2>/dev/null || true
        
        # Crear symlink para compatibilidad
        sudo ln -sf /usr/bin/docker /usr/local/bin/docker-compose-v2
        
        log_success "Alias creado para docker compose"
    fi
}

# Función para probar la instalación
test_installation() {
    log_info "Probando instalación..."
    
    # Probar docker
    if docker --version; then
        log_success "Docker funciona correctamente"
    else
        log_error "Docker no funciona"
        exit 1
    fi
    
    # Probar docker-compose
    if docker compose version &> /dev/null; then
        log_success "Docker Compose v2 funciona correctamente"
        docker compose version
    elif docker-compose --version &> /dev/null; then
        log_success "Docker Compose v1 funciona correctamente"
        docker-compose --version
    else
        log_error "Docker Compose no funciona"
        exit 1
    fi
}

# Función principal
main() {
    log_info "🔧 Solucionando problemas de Docker Compose..."
    echo "=================================================="
    
    # Verificar permisos
    if [[ $EUID -eq 0 ]]; then
        log_error "No ejecutes este script como root"
        exit 1
    fi
    
    # Verificar sudo
    if ! sudo -v; then
        log_error "Se necesitan permisos de sudo"
        exit 1
    fi
    
    # Ejecutar soluciones
    update_docker
    fix_python_deps
    remove_old_compose
    install_compose_v2
    create_compose_alias
    test_installation
    
    echo ""
    log_success "🎉 Problemas solucionados!"
    echo "=================================================="
    log_info "Reinicia la terminal o ejecuta: source ~/.bashrc"
    log_info "Después puedes usar: docker compose o docker-compose"
}

# Ejecutar si se llama directamente
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi 