#!/bin/bash

# Script de despliegue para Docker Compose v2
# Soluciona problemas con docker-compose v1

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

# Detectar comando de docker compose
detect_compose_cmd() {
    if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
        echo "docker compose"
    elif command -v docker-compose >/dev/null 2>&1; then
        echo "docker-compose"
    else
        log_error "Ni 'docker compose' ni 'docker-compose' están disponibles"
        exit 1
    fi
}

# Función para verificar Docker
check_docker() {
    log_info "Verificando Docker..."
    
    if ! command -v docker &> /dev/null; then
        log_error "Docker no está instalado"
        exit 1
    fi
    
    # Detectar comando compose
    COMPOSE_CMD=$(detect_compose_cmd)
    log_info "Usando comando: $COMPOSE_CMD"
    
    log_success "Docker verificado"
}

# Función para verificar archivos
check_files() {
    log_info "Verificando archivos necesarios..."
    
    if [ ! -f "config.yaml" ]; then
        log_error "config.yaml no encontrado"
        log_info "Usa: cp config.yaml.example config.yaml"
        exit 1
    fi
    
    # Usar archivo compose correcto
    if [ -f "docker-compose.v2.yml" ]; then
        COMPOSE_FILE="docker-compose.v2.yml"
    elif [ -f "docker-compose.yml" ]; then
        COMPOSE_FILE="docker-compose.yml"
    else
        log_error "No se encontró archivo docker-compose"
        exit 1
    fi
    
    log_info "Usando archivo: $COMPOSE_FILE"
    log_success "Archivos verificados"
}

# Crear directorios
create_directories() {
    log_info "Creando directorios..."
    mkdir -p logs data
    log_success "Directorios creados"
}

# Construir imagen
build_image() {
    log_info "Construyendo imagen..."
    $COMPOSE_CMD -f "$COMPOSE_FILE" build --no-cache
    log_success "Imagen construida"
}

# Iniciar contenedor
start_container() {
    log_info "Iniciando contenedor..."
    $COMPOSE_CMD -f "$COMPOSE_FILE" up -d
    log_success "Contenedor iniciado"
}

# Mostrar estado
show_status() {
    log_info "Estado del contenedor:"
    $COMPOSE_CMD -f "$COMPOSE_FILE" ps
    echo ""
    log_info "Logs recientes:"
    $COMPOSE_CMD -f "$COMPOSE_FILE" logs --tail=10 scheduler
}

# Mostrar logs
show_logs() {
    log_info "Mostrando logs (Ctrl+C para salir):"
    $COMPOSE_CMD -f "$COMPOSE_FILE" logs -f scheduler
}

# Reiniciar contenedor
restart_container() {
    log_info "Reiniciando contenedor..."
    $COMPOSE_CMD -f "$COMPOSE_FILE" restart scheduler
    log_success "Contenedor reiniciado"
}

# Detener contenedor
stop_container() {
    log_info "Deteniendo contenedor..."
    $COMPOSE_CMD -f "$COMPOSE_FILE" down
    log_success "Contenedor detenido"
}

# Limpiar
cleanup() {
    log_warning "¿Eliminar contenedor y volúmenes? (y/N)"
    read -r response
    if [[ "$response" =~ ^([yY][eE][sS]|[yY])$ ]]; then
        log_info "Limpiando..."
        $COMPOSE_CMD -f "$COMPOSE_FILE" down --rmi all --volumes
        log_success "Limpieza completada"
    else
        log_info "Operación cancelada"
    fi
}

# Función principal
deploy() {
    log_info "🚀 Despliegue con Docker Compose v2"
    echo "================================================"
    
    check_docker
    check_files
    create_directories
    build_image
    start_container
    
    echo ""
    log_success "🎉 Despliegue completado!"
    echo "================================================"
    show_status
    echo ""
    log_info "Comandos disponibles:"
    log_info "  $0 logs     - Ver logs en tiempo real"
    log_info "  $0 status   - Ver estado"
    log_info "  $0 restart  - Reiniciar"
    log_info "  $0 stop     - Detener"
}

# Mostrar ayuda
show_help() {
    echo "Uso: $0 [COMANDO]"
    echo ""
    echo "Comandos disponibles:"
    echo "  deploy    - Despliega el proyecto"
    echo "  build     - Construye la imagen"
    echo "  start     - Inicia el contenedor"
    echo "  stop      - Detiene el contenedor"
    echo "  restart   - Reinicia el contenedor"
    echo "  status    - Muestra estado"
    echo "  logs      - Muestra logs"
    echo "  cleanup   - Limpia todo"
    echo "  help      - Muestra esta ayuda"
}

# Inicializar variables
COMPOSE_CMD=""
COMPOSE_FILE=""

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
        check_files
        start_container
        ;;
    stop)
        check_docker
        check_files
        stop_container
        ;;
    restart)
        check_docker
        check_files
        restart_container
        ;;
    status)
        check_docker
        check_files
        show_status
        ;;
    logs)
        check_docker
        check_files
        show_logs
        ;;
    cleanup)
        check_docker
        check_files
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