import os
import pandas as pd
from kubernetes import client, config
import json
import re

def parse_k8s_quantity(quantity_str):
    """
    Parses a Kubernetes quantity string (e.g., '100m', '512Mi', '1Gi') into a numeric value.
    Returns the value in base units (cores for CPU, bytes for memory).
    """
    if not isinstance(quantity_str, str):
        return 0
    
    quantity_str = quantity_str.strip()
    match = re.match(r'^(\d+(\.\d+)?)(E|P|T|G|M|k|Ei|Pi|Ti|Gi|Mi|Ki|m)?$', quantity_str)
    if not match:
        return 0 # Cannot parse
    
    value, _, suffix = match.groups()
    value = float(value)
    
    multipliers = {
        'E': 10**18, 'P': 10**15, 'T': 10**12, 'G': 10**9, 'M': 10**6, 'k': 10**3,
        'Ei': 2**60, 'Pi': 2**50, 'Ti': 2**40, 'Gi': 2**30, 'Mi': 2**20, 'Ki': 2**10,
        'm': 10**-3
    }
    
    if suffix in multipliers:
        value *= multipliers[suffix]
        
    return value

def get_node_details(api_client):
    """
    Fetches details for all Node objects in the cluster.
    """
    nodes = []
    try:
        response = api_client.list_node()
        for node in response.items:
            mem_bytes = parse_k8s_quantity(node.status.allocatable.get('memory', '0'))
            mem_gib = round(mem_bytes / (1024**3), 2) if mem_bytes > 0 else 0
            # Extracting key details from the node object
            nodes.append({
                'name': node.metadata.name,
                'status': node.status.conditions[-1].type if node.status.conditions else 'Unknown',
                'instance_type': node.metadata.labels.get('beta.kubernetes.io/instance-type', 'N/A'),
                'zone': node.metadata.labels.get('topology.kubernetes.io/zone', 'N/A'),
                'os_image': node.status.node_info.os_image,
                'kernel_version': node.status.node_info.kernel_version,
                'kubelet_version': node.status.node_info.kubelet_version,
                'allocatable_cpu': node.status.allocatable.get('cpu', '0'),
                'allocatable_memory_gib': f"{mem_gib} GiB",
            })
    except client.ApiException as e:
        print(f"Error fetching nodes: {e}")
    return nodes

def get_pod_details(api_client):
    """
    Fetches details for all Pod objects across all namespaces.
    """
    pods = []
    try:
        response = api_client.list_pod_for_all_namespaces()
        for pod in response.items:
            # Extracting key details from the pod object
            containers = pod.spec.containers
            container_images = [c.image for c in containers]
            pods.append({
                'namespace': pod.metadata.namespace,
                'name': pod.metadata.name,
                'status': pod.status.phase,
                'pod_ip': pod.status.pod_ip,
                'node_name': pod.spec.node_name,
                'service_account': pod.spec.service_account_name,
                'container_images': ", ".join(container_images),
            })
    except client.ApiException as e:
        print(f"Error fetching pods: {e}")
    return pods

def get_deployment_details(api_client):
    """
    Fetches details for all Deployment objects across all namespaces.
    """
    deployments = []
    try:
        response = api_client.list_deployment_for_all_namespaces()
        for deployment in response.items:
            # Extracting key details from the deployment object
            containers = deployment.spec.template.spec.containers
            container_images = [c.image for c in containers]

            mounts = []
            for c in containers:
                if c.volume_mounts:
                    for vm in c.volume_mounts:
                        mounts.append(f"{c.name}:{vm.mount_path} -> {vm.name}")

            deployments.append({
                'namespace': deployment.metadata.namespace,
                'name': deployment.metadata.name,
                'replicas_desired': deployment.spec.replicas,
                'replicas_ready': deployment.status.ready_replicas or 0,
                'strategy': deployment.spec.strategy.type,
                'container_images': ", ".join(container_images),
                'volume_mounts': " | ".join(mounts),
            })
    except client.ApiException as e:
        print(f"Error fetching deployments: {e}")
    return deployments

def get_service_details(api_client):
    """
    Fetches details for all Service objects across all namespaces.
    """
    services = []
    try:
        response = api_client.list_service_for_all_namespaces()
        for service in response.items:
            # Extracting key details from the service object
            ports = [f"{p.name}:{p.port}/{p.protocol}" for p in service.spec.ports] if service.spec.ports else []
            load_balancer_ip = ''
            if service.status.load_balancer and service.status.load_balancer.ingress:
                load_balancer_ip = service.status.load_balancer.ingress[0].ip
            
            services.append({
                'namespace': service.metadata.namespace,
                'name': service.metadata.name,
                'type': service.spec.type,
                'cluster_ip': service.spec.cluster_ip,
                'external_ip': load_balancer_ip or 'N/A',
                'ports': ", ".join(ports),
                'selector': str(service.spec.selector),
            })
    except client.ApiException as e:
        print(f"Error fetching services: {e}")
    return services

def get_statefulset_details(api_client):
    """
    Fetches details for all StatefulSet objects across all namespaces.
    """
    statefulsets = []
    try:
        # Use the appropriate method from the AppsV1Api client
        response = api_client.list_stateful_set_for_all_namespaces()
        for ss in response.items:
            # Extracting key details from the statefulset object
            containers = ss.spec.template.spec.containers
            container_images = [c.image for c in containers]

            volume_templates = json.dumps(
                api_client_instance.sanitize_for_serialization(ss.spec.volume_claim_templates)
            ) if ss.spec.volume_claim_templates else '[]'

            statefulsets.append({
                'namespace': ss.metadata.namespace,
                'name': ss.metadata.name,
                'replicas_desired': ss.spec.replicas,
                'replicas_ready': ss.status.ready_replicas or 0,
                'service_name': ss.spec.service_name,
                'container_images': ", ".join(container_images),
                'volume_claim_templates': volume_templates,
            })
    except client.ApiException as e:
        print(f"Error fetching statefulsets: {e}")
    return statefulsets

def get_daemonset_details(api_client):
    """
    Fetches details for all DaemonSet objects across all namespaces.
    """
    daemonsets = []
    try:
        response = api_client.list_daemon_set_for_all_namespaces()
        for ds in response.items:
            containers = ds.spec.template.spec.containers
            container_images = [c.image for c in containers]

            resources = []
            for c in containers:
                if c.resources and (c.resources.requests or c.resources.limits):
                    req = c.resources.requests or {}
                    lim = c.resources.limits or {}
                    resources.append(
                        f"{c.name}(req: cpu={req.get('cpu','-')},mem={req.get('memory','-')}; "
                        f"lim: cpu={lim.get('cpu','-')},mem={lim.get('memory','-')})"
                    )

            daemonsets.append({
                'namespace': ds.metadata.namespace,
                'name': ds.metadata.name,
                'desired_scheduled': ds.status.desired_number_scheduled,
                'current_scheduled': ds.status.current_number_scheduled,
                'ready': ds.status.number_ready,
                'container_images': ", ".join(container_images),
                'container_resources': " | ".join(resources),
            })
    except client.ApiException as e:
        print(f"Error fetching daemonsets: {e}")
    return daemonsets

def get_job_details(api_client):
    """
    Fetches details for all Job objects across all namespaces.
    """
    jobs = []
    try:
        response = api_client.list_job_for_all_namespaces()
        for job in response.items:
            containers = job.spec.template.spec.containers
            container_images = [c.image for c in containers]
            jobs.append({
                'namespace': job.metadata.namespace,
                'name': job.metadata.name,
                'completions': job.spec.completions,
                'succeeded': job.status.succeeded or 0,
                'failed': job.status.failed or 0,
                'container_images': ", ".join(container_images),
                'start_time': job.status.start_time,
            })
    except client.ApiException as e:
        print(f"Error fetching jobs: {e}")
    return jobs

def get_cronjob_details(api_client):
    """
    Fetches details for all CronJob objects across all namespaces.
    """
    cronjobs = []
    try:
        response = api_client.list_cron_job_for_all_namespaces()
        for cj in response.items:
            containers = cj.spec.job_template.spec.template.spec.containers
            container_images = [c.image for c in containers]
            cronjobs.append({
                'namespace': cj.metadata.namespace,
                'name': cj.metadata.name,
                'schedule': cj.spec.schedule,
                'suspend': cj.spec.suspend,
                'last_schedule_time': cj.status.last_schedule_time,
                'container_images': ", ".join(container_images),
                'active_jobs': len(cj.status.active) if cj.status.active else 0,
                'concurrency_policy': cj.spec.concurrency_policy,
                'restart_policy': cj.spec.job_template.spec.template.spec.restart_policy,
            })
    except client.ApiException as e:
        print(f"Error fetching cronjobs: {e}")
    return cronjobs

def get_pv_details(api_client):
    """
    Fetches details for all PersistentVolume (PV) objects in the cluster.
    """
    pvs = []
    try:
        response = api_client.list_persistent_volume()
        for pv in response.items:
            pvs.append({
                'name': pv.metadata.name,
                'capacity': pv.spec.capacity.get('storage', 'N/A'),
                'access_modes': ", ".join(pv.spec.access_modes) if pv.spec.access_modes else "",
                'reclaim_policy': pv.spec.persistent_volume_reclaim_policy,
                'status': pv.status.phase,
                'storage_class': pv.spec.storage_class_name,
                'claim_namespace': pv.spec.claim_ref.namespace if pv.spec.claim_ref else 'N/A',
                'claim_name': pv.spec.claim_ref.name if pv.spec.claim_ref else 'N/A',
            })
    except client.ApiException as e:
        print(f"Error fetching persistent volumes: {e}")
    return pvs

def get_namespace_details(api_client):
    """Fetches details for all Namespace objects."""
    namespaces = []
    try:
        response = api_client.list_namespace()
        for ns in response.items:
            labels = json.dumps(ns.metadata.labels) if ns.metadata.labels else '{}'
            namespaces.append({
                'name': ns.metadata.name,
                'status': ns.status.phase,
                'creation_timestamp': ns.metadata.creation_timestamp,
                'labels': labels,
            })
    except client.ApiException as e:
        print(f"Error fetching namespaces: {e}")
    return namespaces

def get_secret_details(api_client):
    """Fetches metadata for all Secret objects."""
    secrets = []
    try:
        response = api_client.list_secret_for_all_namespaces()
        for secret in response.items:
            secrets.append({
                'namespace': secret.metadata.namespace,
                'name': secret.metadata.name,
                'type': secret.type,
                'data_keys': ", ".join(secret.data.keys()) if secret.data else "",
            })
    except client.ApiException as e:
        print(f"Error fetching secrets: {e}")
    return secrets

def get_configmap_details(api_client):
    """Fetches metadata for all ConfigMap objects."""
    configmaps = []
    try:
        response = api_client.list_config_map_for_all_namespaces()
        for cm in response.items:
            data_count = 0
            data_size_bytes = 0
            data_summary = ""
            
            if cm.data:
                # Calculate the number of keys
                data_count = len(cm.data)
                
                # Calculate the total size of the data values in bytes
                data_size_bytes = sum(len(str(v).encode('utf-8')) for v in cm.data.values())
                
                # Create a summary of the first few keys
                keys = list(cm.data.keys())
                summary_keys = keys[:3] # Show first 3 keys
                data_summary = ", ".join(summary_keys)
                if len(keys) > 3:
                    data_summary += ", ..."
            configmaps.append({
                'namespace': cm.metadata.namespace,
                'name': cm.metadata.name,
                'data_count': data_count,
                'data_size_bytes': data_size_bytes,
                'data_summary': data_summary,
                'data_keys': ", ".join(cm.data.keys()) if cm.data else "",
            })
    except client.ApiException as e:
        print(f"Error fetching configmaps: {e}")
    return configmaps
def get_pvc_details(api_client):
    """Fetches details for all PersistentVolumeClaim objects."""
    pvcs = []
    try:
        response = api_client.list_persistent_volume_claim_for_all_namespaces()
        for pvc in response.items:
            pvcs.append({
                'namespace': pvc.metadata.namespace,
                'name': pvc.metadata.name,
                'status': pvc.status.phase,
                'capacity_request': pvc.spec.resources.requests.get('storage', 'N/A') if pvc.spec.resources.requests else 'N/A',
                'access_modes': ", ".join(pvc.spec.access_modes) if pvc.spec.access_modes else "",
                'storage_class': pvc.spec.storage_class_name,
                'volume_name': pvc.spec.volume_name,
                'volume_mode': pvc.spec.volume_mode,
            })
    except client.ApiException as e:
        print(f"Error fetching PVCs: {e}")
    return pvcs

def get_ingress_details(api_client):
    """Fetches details for all Ingress objects."""
    ingresses = []
    try:
        response = api_client.list_ingress_for_all_namespaces()
        for ingress in response.items:
            hosts = [rule.host for rule in ingress.spec.rules] if ingress.spec.rules else []
            # Extract TLS secret names from all tls entries
            tls_secrets = [tls.secret_name for tls in ingress.spec.tls if tls.secret_name] if ingress.spec.tls else []
            
            # Format annotations as a JSON string for readability
            annotations = json.dumps(ingress.metadata.annotations) if ingress.metadata.annotations else '{}'

            load_balancer_ips = [i.ip for i in ingress.status.load_balancer.ingress] if ingress.status.load_balancer.ingress else []
            ingresses.append({
                'namespace': ingress.metadata.namespace,
                'name': ingress.metadata.name,
                'class': ingress.spec.ingress_class_name or 'N/A',
                'hosts': ", ".join(hosts),
                'load_balancer_ips': ", ".join(load_balancer_ips),
                'tls_secret': ", ".join(tls_secrets),
                'annotations': annotations,
            })
    except client.ApiException as e:
        print(f"Error fetching ingresses: {e}")
    return ingresses

def get_networkpolicy_details(api_client):
    """Fetches details for all NetworkPolicy objects."""
    netpols = []
    api_client_instance = client.ApiClient()
    try:
        response = api_client.list_network_policy_for_all_namespaces()
        for np in response.items:
            ingress_rules = json.dumps(api_client_instance.sanitize_for_serialization(np.spec.ingress)) if np.spec.ingress else '[]'
            egress_rules = json.dumps(api_client_instance.sanitize_for_serialization(np.spec.egress)) if np.spec.egress else '[]'
            
            netpols.append({
                'namespace': np.metadata.namespace,
                'name': np.metadata.name,
                'pod_selector': str(np.spec.pod_selector.match_labels) if np.spec.pod_selector else '{}',
                'policy_types': ", ".join(np.spec.policy_types) if np.spec.policy_types else "",
                'ingress_rules': ingress_rules,
                'egress_rules': egress_rules,
            })
    except client.ApiException as e:
        print(f"Error fetching network policies: {e}")
    return netpols

def get_hpa_details(api_client):
    """Fetches details for all HorizontalPodAutoscaler objects."""
    hpas = []
    api_client_instance = client.ApiClient()
    try:
        response = api_client.list_horizontal_pod_autoscaler_for_all_namespaces()
        for hpa in response.items:
            metrics = json.dumps(api_client_instance.sanitize_for_serialization(hpa.spec.metrics)) if hpa.spec.metrics else '[]'
            hpas.append({
                'namespace': hpa.metadata.namespace,
                'name': hpa.metadata.name,
                'scale_target_ref': f"{hpa.spec.scale_target_ref.kind}/{hpa.spec.scale_target_ref.name}",
                'min_replicas': hpa.spec.min_replicas,
                'max_replicas': hpa.spec.max_replicas,
                'current_replicas': hpa.status.current_replicas,
                'desired_replicas': hpa.status.desired_replicas,
                'metrics': metrics,
            })
    except client.ApiException as e:
        print(f"Error fetching HPAs: {e}")
    return hpas

def get_role_details(api_client):
    """Fetches details for all Role objects."""
    roles = []
    try:
        response = api_client.list_role_for_all_namespaces()
        for role in response.items:
            rules_summary = [f"[{','.join(rule.api_groups)}][{','.join(rule.resources)}][{','.join(rule.verbs)}]" for rule in role.rules] if role.rules else []
            annotations = json.dumps(role.metadata.annotations) if role.metadata.annotations else '{}'
            roles.append({
                'namespace': role.metadata.namespace,
                'name': role.metadata.name,
                'rules': " | ".join(rules_summary),
                'annotations': annotations,
            })
    except client.ApiException as e:
        print(f"Error fetching roles: {e}")
    return roles

def get_rolebinding_details(api_client):
    """Fetches details for all RoleBinding objects."""
    bindings = []
    try:
        response = api_client.list_role_binding_for_all_namespaces()
        for rb in response.items:
            subjects = [f"{s.kind}:{s.name}" for s in rb.subjects] if rb.subjects else []
            bindings.append({
                'namespace': rb.metadata.namespace,
                'name': rb.metadata.name,
                'role_ref': f"{rb.role_ref.kind}/{rb.role_ref.name}",
                'subjects': ", ".join(subjects),
            })
    except client.ApiException as e:
        print(f"Error fetching role bindings: {e}")
    return bindings
def get_resourcequota_details(api_client):
    """Fetches details for all ResourceQuota objects."""
    quotas = []
    try:
        response = api_client.list_resource_quota_for_all_namespaces()
        for quota in response.items:
            quotas.append({
                'namespace': quota.metadata.namespace,
                'name': quota.metadata.name,
                'hard_limits': json.dumps(quota.spec.hard) if quota.spec.hard else '{}',
                'used': json.dumps(quota.status.used) if quota.status.used else '{}',
            })
    except client.ApiException as e:
        print(f"Error fetching resource quotas: {e}")
    return quotas

def get_limitrange_details(api_client):
    """Fetches details for all LimitRange objects."""
    ranges = []
    try:
        response = api_client.list_limit_range_for_all_namespaces()
        for lr in response.items:
            if not lr.spec.limits:
                continue
            
            for item in lr.spec.limits:
                ranges.append({
                    'name': lr.metadata.name,
                    'namespace': lr.metadata.namespace,
                    'type': item.type,
                    'max_cpu': item.max.get('cpu', 'N/A') if item.max else 'N/A',
                    'max_mem': item.max.get('memory', 'N/A') if item.max else 'N/A',
                    'min_cpu': item.min.get('cpu', 'N/A') if item.min else 'N/A',
                    'min_mem': item.min.get('memory', 'N/A') if item.min else 'N/A',
                    'default_cpu': item.default.get('cpu', 'N/A') if item.default else 'N/A',
                    'default_mem': item.default.get('memory', 'N/A') if item.default else 'N/A',
                    'default_request_cpu': item.default_request.get('cpu', 'N/A') if item.default_request else 'N/A',
                    'default_request_mem': item.default_request.get('memory', 'N/A') if item.default_request else 'N/A',
                })
    except client.ApiException as e:
        print(f"Error fetching limit ranges: {e}")
    return ranges

def get_pdb_details(api_client):
    """Fetches details for all PodDisruptionBudget objects."""
    pdbs = []
    try:
        response = api_client.list_pod_disruption_budget_for_all_namespaces()
        for pdb in response.items:
            pdbs.append({
                'namespace': pdb.metadata.namespace,
                'name': pdb.metadata.name,
                'min_available': pdb.spec.min_available,
                'max_unavailable': pdb.spec.max_unavailable,
                'selector': str(pdb.spec.selector.match_labels) if pdb.spec.selector else '{}',
                'current_healthy': pdb.status.current_healthy,
                'desired_healthy': pdb.status.desired_healthy,
            })
    except client.ApiException as e:
        print(f"Error fetching PDBs: {e}")
    return pdbs

def get_serviceaccount_details(api_client):
    """
    Fetches details for all ServiceAccount objects.
    """
    service_accounts = []
    try:
        response = api_client.list_service_account_for_all_namespaces()
        for sa in response.items:
            service_accounts.append({
                'namespace': sa.metadata.namespace,
                'name': sa.metadata.name,
                'automount_token': sa.automount_service_account_token,
                'creation_timestamp': sa.metadata.creation_timestamp,
            })
    except client.ApiException as e:
        print(f"Error fetching service accounts: {e}")
    return service_accounts

def main():
    """
    Main function to connect to the cluster, fetch details, and save to CSV.
    """
    try:
        # Load kubeconfig from default location
        config.load_kube_config()
        print("Successfully loaded kubeconfig.")
    except config.ConfigException as e:
        print(f"Could not load kubeconfig: {e}")
        return

    # Create API clients for different Kubernetes API groups
    core_v1 = client.CoreV1Api()
    apps_v1 = client.AppsV1Api()
    batch_v1 = client.BatchV1Api() # New client for Jobs and CronJobs

    networking_v1 = client.NetworkingV1Api()
    #autoscaling_v1 = client.AutoscalingV1Api()

    autoscaling_v2 = client.AutoscalingV2Api()
    rbac_v1 = client.RbacAuthorizationV1Api()
    policy_v1 = client.PolicyV1Api()
    
    output_dir = './gke_assessment_output'
    os.makedirs(output_dir, exist_ok=True)
    print(f"Output will be saved to: {output_dir}")

    tasks = {
        'Nodes': (get_node_details, core_v1, 'gke_nodes.csv'),
        'Namespaces': (get_namespace_details, core_v1, 'gke_namespaces.csv'),
        'Pods': (get_pod_details, core_v1, 'gke_pods.csv'),
        'Services': (get_service_details, core_v1, 'gke_services.csv'),
        'Deployments': (get_deployment_details, apps_v1, 'gke_deployments.csv'),
        'ServiceAccounts': (get_serviceaccount_details, core_v1, 'gke_serviceaccounts.csv'),
        'StatefulSets': (get_statefulset_details, apps_v1, 'gke_statefulsets.csv'),
        'DaemonSets': (get_daemonset_details, apps_v1, 'gke_daemonsets.csv'),
        'Jobs': (get_job_details, batch_v1, 'gke_jobs.csv'),
        'CronJobs': (get_cronjob_details, batch_v1, 'gke_cronjobs.csv'),
        'PersistentVolumes': (get_pv_details, core_v1, 'gke_pvs.csv'),
        'PersistentVolumeClaims': (get_pvc_details, core_v1, 'gke_pvcs.csv'),
        'Secrets': (get_secret_details, core_v1, 'gke_secrets.csv'),
        'ConfigMaps': (get_configmap_details, core_v1, 'gke_configmaps.csv'),
        'Ingresses': (get_ingress_details, networking_v1, 'gke_ingresses.csv'),
        'NetworkPolicies': (get_networkpolicy_details, networking_v1, 'gke_networkpolicies.csv'),
        'HPAs': (get_hpa_details, autoscaling_v2, 'gke_hpas.csv'),
        'Roles': (get_role_details, rbac_v1, 'gke_roles.csv'),
        'RoleBindings': (get_rolebinding_details, rbac_v1, 'gke_rolebindings.csv'),
        'ResourceQuotas': (get_resourcequota_details, core_v1, 'gke_resourcequotas.csv'),
        'LimitRanges': (get_limitrange_details, core_v1, 'gke_limitranges.csv'),
        'PDBs': (get_pdb_details, policy_v1, 'gke_pdbs.csv'),
    }
    for resource_name, (func, api_client, filename) in tasks.items():
        print(f"Fetching {resource_name}...")
        data = func(api_client)
        if data:
            pd.DataFrame(data).to_csv(f'{output_dir}/{filename}', index=False)
        else:
            print(f"No {resource_name} found or an error occurred.")
    
    print("\nAssessment complete! All CSV files have been generated.")


if __name__ == '__main__':
    main()