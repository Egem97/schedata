#!/bin/bash

# Script de despliegue para VPS con Docker
# Autor: Tu Nombre
# Descripción: Despliega el sistema automatizado de procesamiento de datos

set -e  # Salir en caso de error

# Colores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Función para mostrar mensajes
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

# Función para verificar si Docker está instalado
check_docker() {
    log_info "Verificando Docker..."
    if ! command -v docker &> /dev/null; then
        log_error "Docker no está instalado. Por favor instala Docker primero."
        exit 1
    fi
    
    if ! command -v docker-compose &> /dev/null; then
        log_error "Docker Compose no está instalado. Por favor instala Docker Compose primero."
        exit 1
    fi
    
    log_success "Docker y Docker Compose están instalados"
}

# Función para verificar archivos necesarios
check_files() {
    log_info "Verificando archivos necesarios..."
    
    if [ ! -f "config.yaml" ]; then
        log_error "config.yaml no encontrado. Por favor crea el archivo de configuración."
        log_info "Puedes usar config.yaml.example como plantilla"
        exit 1
    fi
    
    if [ ! -f "requirements.txt" ]; then
        log_error "requirements.txt no encontrado"
        exit 1
    fi
    
    if [ ! -f "scheduler.py" ]; then
        log_error "scheduler.py no encontrado"
        exit 1
    fi
    
    log_success "Todos los archivos necesarios están presentes"
}

# Función para crear directorios necesarios
create_directories() {
    log_info "Creando directorios necesarios..."
    mkdir -p logs
    mkdir -p data
    log_success "Directorios creados"
}

# Función para construir la imagen
build_image() {
    log_info "Construyendo imagen Docker..."
    docker-compose build --no-cache
    log_success "Imagen construida exitosamente"
}

# Función para iniciar el contenedor
start_container() {
    log_info "Iniciando contenedor..."
    docker-compose up -d
    log_success "Contenedor iniciado"
}

# Función para mostrar el estado
show_status() {
    log_info "Estado del contenedor:"
    docker-compose ps
    echo ""
    log_info "Logs recientes:"
    docker-compose logs --tail=10 scheduler
}

# Función para mostrar logs
show_logs() {
    log_info "Mostrando logs en tiempo real (Ctrl+C para salir):"
    docker-compose logs -f scheduler
}

# Función para reiniciar el contenedor
restart_container() {
    log_info "Reiniciando contenedor..."
    docker-compose restart scheduler
    log_success "Contenedor reiniciado"
}

# Función para detener el contenedor
stop_container() {
    log_info "Deteniendo contenedor..."
    docker-compose down
    log_success "Contenedor detenido"
}

# Función para limpiar todo
cleanup() {
    log_warning "Esto eliminará el contenedor y la imagen. ¿Estás seguro? (y/N)"
    read -r response
    if [[ "$response" =~ ^([yY][eE][sS]|[yY])$ ]]; then
        log_info "Limpiando contenedores e imágenes..."
        docker-compose down --rmi all --volumes
        log_success "Limpieza completada"
    else
        log_info "Operación cancelada"
    fi
}

# Función para mostrar ayuda
show_help() {
    echo "Uso: $0 [COMANDO]"
    echo ""
    echo "Comandos disponibles:"
    echo "  deploy    - Despliega el proyecto completo"
    echo "  build     - Construye la imagen Docker"
    echo "  start     - Inicia el contenedor"
    echo "  stop      - Detiene el contenedor"
    echo "  restart   - Reinicia el contenedor"
    echo "  status    - Muestra el estado del contenedor"
    echo "  logs      - Muestra logs en tiempo real"
    echo "  cleanup   - Limpia contenedores e imágenes"
    echo "  help      - Muestra esta ayuda"
    echo ""
    echo "Ejemplo: $0 deploy"
}

# Función principal de despliegue
deploy() {
    log_info "🚀 Iniciando despliegue del Sistema Automatizado"
    echo "================================================="
    
    check_docker
    check_files
    create_directories
    build_image
    start_container
    
    echo ""
    log_success "🎉 Despliegue completado exitosamente!"
    echo "================================================="
    show_status
    echo ""
    log_info "Para ver los logs en tiempo real: $0 logs"
    log_info "Para ver el estado: $0 status"
    log_info "Para reiniciar: $0 restart"
}

# Procesar argumentos
case "${1:-help}" in
    deploy)
        deploy
        ;;
    build)
        check_docker
        check_files
        build_image
        ;;
    start)
        check_docker
        start_container
        ;;
    stop)
        check_docker
        stop_container
        ;;
    restart)
        check_docker
        restart_container
        ;;
    status)
        check_docker
        show_status
        ;;
    logs)
        check_docker
        show_logs
        ;;
    cleanup)
        check_docker
        cleanup
        ;;
    help|--help|-h)
        show_help
        ;;
    *)
        log_error "Comando no reconocido: $1"
        show_help
        exit 1
        ;;
esac 