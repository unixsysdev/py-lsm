#!/bin/bash

# Define the services
SERVICES=("vm-storage.py" "vm-insert.py" "vm-select.py")

# Function to start a service
start_service() {
    if pgrep -f "$1" > /dev/null
    then
        echo "$1 is already running."
    else
        echo "Starting $1..."
        python3 "$1" > "$1.log" 2>&1 &
        sleep 2
        if pgrep -f "$1" > /dev/null
        then
            echo "$1 started successfully."
        else
            echo "Failed to start $1. Check $1.log for details."
        fi
    fi
}

# Function to stop a service
stop_service() {
    if pgrep -f "$1" > /dev/null
    then
        echo "Stopping $1..."
        pkill -f "$1"
        sleep 2
        if pgrep -f "$1" > /dev/null
        then
            echo "Failed to stop $1. You may need to kill it manually."
        else
            echo "$1 stopped successfully."
        fi
    else
        echo "$1 is not running."
    fi
}

# Function to restart a service
restart_service() {
    stop_service "$1"
    start_service "$1"
}

# Function to check the status of a service
status_service() {
    if pgrep -f "$1" > /dev/null
    then
        echo "$1 is running."
    else
        echo "$1 is not running."
    fi
}

# Function to start all services
start_all() {
    for service in "${SERVICES[@]}"; do
        start_service "$service"
    done
}

# Function to stop all services
stop_all() {
    for service in "${SERVICES[@]}"; do
        stop_service "$service"
    done
}

# Function to restart all services
restart_all() {
    stop_all
    start_all
}

# Function to check status of all services
status_all() {
    for service in "${SERVICES[@]}"; do
        status_service "$service"
    done
}

# Main script logic
case "$1" in
    start)
        if [ "$2" ]; then
            start_service "$2"
        else
            start_all
        fi
        ;;
    stop)
        if [ "$2" ]; then
            stop_service "$2"
        else
            stop_all
        fi
        ;;
    restart)
        if [ "$2" ]; then
            restart_service "$2"
        else
            restart_all
        fi
        ;;
    status)
        if [ "$2" ]; then
            status_service "$2"
        else
            status_all
        fi
        ;;
    *)
        echo "Usage: $0 {start|stop|restart|status} [service_name]"
        echo "Available services: ${SERVICES[*]}"
        exit 1
        ;;
esac

exit 0
