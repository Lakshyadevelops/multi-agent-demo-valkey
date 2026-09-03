"""
Synthetic telemetry provider simulating 3 distinct e-commerce microservices incident scenarios:
1. Scenario 1: Cache TTL Collapse (Ingress 504 on checkout-gw -> Postgres pool exhaustion)
2. Scenario 2: Container OOMKill Cascade (HTTP 502 on payment-service -> Pod OOMKills)
3. Scenario 3: Database Deadlock Migration (Latency spike on order-service -> Exclusive table lock)
"""
from typing import Dict, Any, List
import time

class TelemetryMock:
    @staticmethod
    def get_scenarios() -> List[Dict[str, str]]:
        return [
            {
                "id": "scenario_cache_ttl",
                "name": "Scenario 1: Redis Session Cache TTL Collapse",
                "service": "checkout-gw",
                "alert_type": "ALERT_INGRESS_TIMEOUT",
                "description": "Checkout service throwing 504 timeouts due to downstream auth session connection storm."
            },
            {
                "id": "scenario_k8s_oom",
                "name": "Scenario 2: Payment Service Pod OOMKill Cascade",
                "service": "payment-service",
                "alert_type": "ALERT_PAYMENT_FAILURE",
                "description": "Payment service dropping requests (502 Bad Gateway) due to container memory limit constriction."
            },
            {
                "id": "scenario_db_deadlock",
                "name": "Scenario 3: Unindexed Migration Table Lock & Deadlock",
                "service": "order-service",
                "alert_type": "ALERT_LATENCY_SPIKE",
                "description": "Order placement p99 latency spiking to 18s due to an unindexed foreign key migration locking the table."
            }
        ]

    @staticmethod
    def get_trace_data(service: str, scenario: str = "scenario_cache_ttl") -> Dict[str, Any]:
        now = int(time.time() * 1000)
        
        if scenario == "scenario_k8s_oom" or service == "payment-service":
            return {
                "trace_id": "tr-pay-441a",
                "root_service": "payment-service",
                "timestamp": now - 90000,
                "status_code": 502,
                "duration_ms": 3200,
                "spans": [
                    {
                        "span_id": "span_pay_ingress",
                        "service": "payment-service",
                        "operation": "POST /api/v1/payments/charge",
                        "duration_ms": 3200,
                        "error": "HTTP 502 Bad Gateway: Connection reset by peer",
                    },
                    {
                        "span_id": "span_pay_process",
                        "service": "payment-service",
                        "parent_span": "span_pay_ingress",
                        "operation": "payment.crypto.verify_signature",
                        "duration_ms": 1100,
                        "error": "SIGKILL: Process terminated abruptly by kernel",
                    }
                ]
            }

        elif scenario == "scenario_db_deadlock" or service == "order-service":
            return {
                "trace_id": "tr-ord-883c",
                "root_service": "order-service",
                "timestamp": now - 110000,
                "status_code": 504,
                "duration_ms": 18500,
                "spans": [
                    {
                        "span_id": "span_order_ingress",
                        "service": "order-service",
                        "operation": "POST /api/v1/orders/create",
                        "duration_ms": 18500,
                        "error": "HTTP 504 Gateway Timeout",
                    },
                    {
                        "span_id": "span_order_db",
                        "service": "order-service",
                        "parent_span": "span_order_ingress",
                        "operation": "db.transaction.commit",
                        "duration_ms": 18200,
                        "error": "QueryTimeout: waiting for lock on table 'orders'",
                    }
                ]
            }

        else: # scenario_cache_ttl
            return {
                "trace_id": "tr-7f89a2b1c",
                "root_service": "checkout-gw",
                "timestamp": now - 120000,
                "status_code": 504,
                "duration_ms": 12450,
                "spans": [
                    {
                        "span_id": "span_ingress_01",
                        "service": "checkout-gw",
                        "operation": "POST /api/v1/checkout",
                        "duration_ms": 12450,
                        "error": "HTTP 504 Gateway Timeout",
                    },
                    {
                        "span_id": "span_auth_02",
                        "service": "auth-api",
                        "parent_span": "span_ingress_01",
                        "operation": "POST /api/v1/auth/verify_session",
                        "duration_ms": 12380,
                        "error": "Timeout awaiting response",
                    },
                    {
                        "span_id": "span_db_pool_03",
                        "service": "auth-api",
                        "parent_span": "span_auth_02",
                        "operation": "db.pool.acquire_connection",
                        "duration_ms": 12000,
                        "error": "TimeoutError: Pool exhausted (100/100 active)",
                    }
                ]
            }

    @staticmethod
    def get_database_metrics(service: str, scenario: str = "scenario_cache_ttl") -> Dict[str, Any]:
        if scenario == "scenario_k8s_oom" or service == "payment-service":
            return {
                "target_service": service,
                "database_cluster": "postgres-prod-payment",
                "pool_max_connections": 100,
                "pool_active_connections": 14,
                "pool_waiting_requests": 0,
                "query_metrics": {
                    "p50_latency_ms": 0.45,
                    "p95_latency_ms": 0.90,
                    "p99_latency_ms": 1.10,
                    "active_locks": 0,
                    "deadlocks_detected": 0
                },
                "summary": "Database engine and connection pool are completely healthy and idle."
            }

        elif scenario == "scenario_db_deadlock" or service == "order-service":
            return {
                "target_service": service,
                "database_cluster": "postgres-prod-orders",
                "pool_max_connections": 100,
                "pool_active_connections": 45,
                "pool_waiting_requests": 88,
                "query_metrics": {
                    "p50_latency_ms": 420.0,
                    "p95_latency_ms": 14200.0,
                    "p99_latency_ms": 18200.0,
                    "active_locks": 8,
                    "deadlocks_detected": 14
                },
                "summary": "CRITICAL: Multiple transactions waiting on ExclusiveLock on table 'orders'. Severe table lock contention detected."
            }

        else: # scenario_cache_ttl
            return {
                "target_service": service,
                "database_cluster": "postgres-prod-auth",
                "pool_max_connections": 100,
                "pool_active_connections": 100,
                "pool_waiting_requests": 482,
                "query_metrics": {
                    "p50_latency_ms": 0.72,
                    "p95_latency_ms": 1.25,
                    "p99_latency_ms": 1.84,
                    "active_locks": 0,
                    "deadlocks_detected": 0
                },
                "summary": "Database engine is healthy and fast (<2ms p99), but client connection pool is 100% saturated."
            }

    @staticmethod
    def get_deploy_history(service: str, scenario: str = "scenario_cache_ttl") -> List[Dict[str, Any]]:
        now = int(time.time() * 1000)

        if scenario == "scenario_k8s_oom" or service == "payment-service":
            return [
                {
                    "commit_sha": "b83910c2",
                    "service": service,
                    "author": "devops@cymbal.retail",
                    "timestamp": now - 720000, # 12 mins ago
                    "title": "chore(k8s): tighten container resource requests and limits from 4Gi to 256Mi",
                    "diff_summary": "deployment.yaml: resources.limits.memory changed from 4Gi to 256Mi",
                    "deploy_status": "DEPLOYED_PROD",
                    "environment": "production"
                }
            ]

        elif scenario == "scenario_db_deadlock" or service == "order-service":
            return [
                {
                    "commit_sha": "9a02fb14",
                    "service": service,
                    "author": "backend-orders@cymbal.retail",
                    "timestamp": now - 900000, # 15 mins ago
                    "title": "feat(orders): run migration adding foreign key constraint without NOT VALID clause",
                    "diff_summary": "V12__add_fk_orders_customer.sql: ALTER TABLE orders ADD CONSTRAINT fk_cust FOREIGN KEY (cust_id) REFERENCES customers(id);",
                    "deploy_status": "DEPLOYED_PROD",
                    "environment": "production"
                }
            ]

        else: # scenario_cache_ttl
            return [
                {
                    "commit_sha": "4e81a0d8",
                    "service": service,
                    "author": "sre-core@cymbal.retail",
                    "timestamp": now - 480000,  # 8 minutes ago
                    "title": "fix(auth): reduce redis session TTL to 0 to force DB re-auth",
                    "diff_summary": "Config key 'AUTH_CACHE_TTL_SECONDS' changed from 3600 to 0",
                    "deploy_status": "DEPLOYED_PROD",
                    "environment": "production"
                }
            ]

    @staticmethod
    def get_kubernetes_metrics(service: str, scenario: str = "scenario_cache_ttl") -> Dict[str, Any]:
        if scenario == "scenario_k8s_oom" or service == "payment-service":
            return {
                "service": service,
                "pod_count": 6,
                "healthy_pods": 2,
                "restarts_last_hour": 14,
                "oom_killed_count": 8,
                "cpu_utilization_pct": 89.2,
                "memory_utilization_pct": 99.8,
                "network_dropped_packets": 0,
                "kube_proxy_sync_errors": 0,
                "status": "CRASH_LOOP_OOM_DETECTED"
            }

        elif scenario == "scenario_db_deadlock" or service == "order-service":
            return {
                "service": service,
                "pod_count": 8,
                "healthy_pods": 8,
                "restarts_last_hour": 0,
                "oom_killed_count": 0,
                "cpu_utilization_pct": 18.0,
                "memory_utilization_pct": 34.5,
                "network_dropped_packets": 0,
                "kube_proxy_sync_errors": 0,
                "status": "HEALTHY"
            }

        else: # scenario_cache_ttl
            return {
                "service": service,
                "pod_count": 8,
                "healthy_pods": 8,
                "restarts_last_hour": 0,
                "oom_killed_count": 0,
                "cpu_utilization_pct": 42.5,
                "memory_utilization_pct": 58.2,
                "network_dropped_packets": 0,
                "kube_proxy_sync_errors": 0,
                "status": "HEALTHY"
            }
