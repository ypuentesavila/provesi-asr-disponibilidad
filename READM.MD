# Experimento ASR de Disponibilidad - Provesi SAS

Este proyecto simula la integración entre un WMS y un ERP A2 Softway, demostrando
tolerancia a fallos mediante patrones como Circuit Breaker, Queue-Based Load Leveling
y Retry Pattern.

## Estructura
- wms/ → cliente que envía pedidos
- erp/ → ERP simulado con fallas controladas
- worker/ → reintenta pedidos fallidos desde la cola

## Ejecución
1. Clonar el repositorio:
   git clone https://github.com/ypuentesavila/provesi-asr-disponibilidad.git

2. En cada EC2:
   - WMS: cd wms && python3 wms_producer.py
   - ERP: cd erp && python3 erp_service.py
   - Worker: cd worker && python3 worker_subscriber.py

3. Base de datos: ejecutar db/create_tables.sql
