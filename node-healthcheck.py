# -*- coding: utf-8 -*-
import kopf
import os
from dataclasses import dataclass
from typing import Dict
from copy import deepcopy
import requests
from typing import List, Any
import json
from cachetools import cached, TTLCache, LRUCache
from kubernetes import client, config
import yaml
from urllib.parse import urlparse


@dataclass
class Config:
    healthchecks_url: str = os.environ.get("HEALTHCHECKS_URL")
    healthchecks_api_key: str = os.environ.get("HEALTHCHECKS_API_KEY")
    healthchecks_custom_headers = json.loads(
        os.environ.get("HEALTHCHECKS_CUSTOM_HEADERS", "{}")
    )

    sync_checks: bool = bool(os.environ.get("sync_checks", "True"))
    interval_timer: int = int(os.environ.get("INTERVAL_TIMER", "3600"))
    idle_timer: int = int(os.environ.get("IDLE_TIMER", "1800"))
    pings_per_interval: int = int(os.environ.get("PINGS_PER_INTERVAL", "2"))

    check_image: str = os.environ.get("CHECK_IMAGE", "curlimages/curl:latest")
    template_path: str = os.environ.get("TEMPLATE_PATH", "templates/")

    check_pod_dns_config_str: str = os.environ.get("POD_DNS_CONFIG", "")
    check_pod_dns_policy: str = os.environ.get("POD_DNS_POLICY", "Default")

    def check_pod_dns(self):
        if self.check_pod_dns_config_str == "":
            return None
        return json.loads(self.check_pod_dns_config_str)

    def get_healthchecks_protocol(self):
        return urlparse(self.healthchecks_url).scheme


class HealthchecksIO(object):
    class UniqueCheckFailed(Exception):
        pass

    def __init__(self, api_url: str, api_key: str, custom_headers: Dict[str, str] = {}):
        self.api_key = api_key
        self.api_url = api_url if not api_url.endswith("/") else api_url[:-1]
        if self.api_url.endswith("api"):
            self.v1_api_url = os.path.join(self.api_url, "v1")
        else:
            self.v1_api_url = os.path.join(self.api_url, "api/v1")

        self.headers = deepcopy(custom_headers)
        self.headers["X-Api-Key"] = self.api_key
        self.headers["Content-type"] = "application/json"

    def create_alert(
        self,
        name: str,
        timeout: int = 86400,
        tags: List[str] = [],
        desc: str = "",
        grace: int = 3600,
        channels: List[str] = ["*"],
        unique: List[str] = ["name"],
    ) -> Dict[str, str]:
        """Creates a healthchecks.io alert"""
        alert_data = json.dumps(
            {
                "name": name,
                "timeout": timeout,
                "tags": " ".join(tags),
                "desc": desc,
                "grace": grace,
                "channels": ",".join(channels),
                "unique": unique,
            }
        )
        response = requests.post(
            headers=self.headers, data=alert_data, url=self._get_v1_url("checks/")
        )
        try:
            response.raise_for_status()
        except requests.HTTPError as error:
            print(response.text)
            raise error
        if response.status_code == 200:
            raise self.UniqueCheckFailed(
                f"Alert {name} failed to create due to unique filter {unique}"
            )
        return response.json()

    def delete_alert(self, uuid: str) -> Dict[str, str]:
        """Deletes an alert by uuid"""
        response = requests.delete(
            headers=self.headers, url=self._get_v1_url(os.path.join("checks", uuid))
        )
        response.raise_for_status()
        return response.json()

    def update_alert(
        self, uuid: str, update_dict: Dict[str, str] = {}
    ) -> Dict[str, str]:
        """Updates an alert by uuid"""
        response = requests.post(
            headers=self.headers,
            data=json.dumps(update_dict),
            url=self._get_v1_url(os.path.join("checks", uuid)),
        )
        try:
            response.raise_for_status()
        except requests.HTTPError as error:
            print(response.text)
            raise error
        return response.json()

    @cached(cache=TTLCache(maxsize=32, ttl=600))
    def get_alert(self, uuid: str) -> Dict[str, str]:
        """Get an alert by UUID"""
        response = requests.get(
            headers=self.headers, url=self._get_v1_url(os.path.join("checks", uuid))
        )
        response.raise_for_status()
        return response.json()

    def pause_alert(self, uuid: str) -> Dict[str, str]:
        """Pause an alert"""
        response = requests.post(
            headers=self.headers,
            data=json.dumps({}),
            url=self._get_v1_url(os.path.join("checks", uuid, "pause")),
        )
        response.raise_for_status()
        return response.json()

    @cached(cache=LRUCache(maxsize=128))
    def _get_v1_url(self, path: str) -> str:
        """Returns a v1 url"""
        return os.path.join(self.v1_api_url, path)


CRD = ["andrewthetechie.github.com", "v1", "nodehealthchecks"]
CONFIG = Config()
HCIO = HealthchecksIO(
    CONFIG.healthchecks_url,
    CONFIG.healthchecks_api_key,
    CONFIG.healthchecks_custom_headers,
)


def spec_to_alert(spec: Dict) -> Dict[str, Any]:
    """Converts a spec from the CRD into a dict we can use to create an alert"""
    alert_data = dict()

    # convert lists to strings
    alert_data["tags"] = " ".join(spec.get("tags", []))
    alert_data["channels"] = ",".join(spec["channels"])

    # rename from friendly names
    alert_data["grace"] = spec.get("grace_period")
    alert_data["desc"] = spec.get("description")

    # copy over other values
    alert_data["timeout"] = spec.get("timeout")

    return alert_data


# TODO: Make the retry automatically. Wasn't there a cool retry library I used before?
def patch_object(name: str, namespace: str, patch_data: Dict, logger) -> bool:
    """Patch our object to update values"""
    response = client.CustomObjectsApi().patch_namespaced_custom_object(
        group=CRD[0],
        version=CRD[1],
        plural=CRD[2],
        namespace=namespace,
        name=name,
        body=patch_data,
    )
    logger.debug(response)
    # TODO: Error handling here
    return True


@cached(cache=LRUCache(maxsize=256))
def _get_cronjob_schedule(timeout: int) -> str:
    """Turns a timeout (seconds int) into a cron style schedule to try to ping N times in that timeout"""
    ping_interval = timeout / CONFIG.pings_per_interval
    print(f"Ping interval: {ping_interval}")

    ping_interval_days = int(ping_interval // (60 * 60 * 24))
    print(f"Ping interval days: {ping_interval_days}")
    if ping_interval_days > 0:
        return f"0 0 */{ int(ping_interval_days) } * *"

    ping_interval_hours = int(ping_interval // (60 * 60))
    print(f"Ping interval hours: {ping_interval_hours}")
    if ping_interval_hours > 0:
        return f"0 */{ int(ping_interval_hours) } * * * "

    ping_interval_minutes = ping_interval // 60
    print(f"Ping interval minutes: {ping_interval_minutes}")
    if ping_interval_minutes > 0:
        if ping_interval_minutes > 30:
            return "*/30 * * * *"
        return f"*/{ int(ping_interval_minutes) } * * * *"
    # default to pinging every 2 minutes
    return "*/2 * * * *"


@cached(cache=LRUCache(maxsize=256))
def _get_cronjob_spec(
    name: str, namespace: str, timeout: int, node: str, ping_url: str
) -> Dict:
    """Returns the spec for a CronJob"""
    with open(os.path.join(CONFIG.template_path, "cronJobTemplate.yaml"), "r") as file:
        spec = yaml.load(file, Loader=yaml.SafeLoader)

    spec["metadata"]["name"] = f"{namespace}-{name}-nhc"
    spec["metadata"]["namespace"] = namespace
    spec["spec"]["schedule"] = _get_cronjob_schedule(timeout)
    spec["spec"]["jobTemplate"]["spec"]["template"]["spec"]["nodeName"] = node
    spec["spec"]["jobTemplate"]["spec"]["template"]["spec"]["containers"][0][
        "name"
    ] = f"{namespace}-{name}-pod"
    spec["spec"]["jobTemplate"]["spec"]["template"]["spec"]["containers"][0][
        "image"
    ] = CONFIG.check_image
    spec["spec"]["jobTemplate"]["spec"]["template"]["spec"]["containers"][0][
        "args"
    ].append(ping_url)

    spec["spec"]["jobTemplate"]["spec"]["template"]["spec"][
        "dnsPolicy"
    ] = CONFIG.check_pod_dns_policy
    pod_dns = CONFIG.check_pod_dns()
    if pod_dns is not None:
        spec["spec"]["jobTemplate"]["spec"]["template"]["spec"]["dnsConfig"] = pod_dns
    return spec


def _create_cronjob(cronjob_spec: Dict, namespace: str):
    """Create a k8s cronjob to curl the healthcheck we created"""
    response = client.BatchV1beta1Api().create_namespaced_cron_job(
        namespace=namespace, body=cronjob_spec
    )
    return response


def _update_cronjob(cronjob_spec: Dict, namespace: str, name: str):
    """Create a k8s cronjob to curl the healthcheck we created"""
    response = client.BatchV1beta1Api().patch_namespaced_cron_job(
        name=name, namespace=namespace, body=cronjob_spec
    )
    return response


def _create_nhc(name: str, namespace: str, logger, spec: Dict) -> str:
    """Creates a nodehealthcheck"""
    logger.debug("Create called for new hc with name %s", name)
    # TODO: handle errors better.
    # Specifically catch an error if the alert name already exists
    global HCIO
    try:
        alert = HCIO.create_alert(name=f"{namespace}-{name}", **spec_to_alert(spec))
    except Exception as error:
        raise kopf.PermanentError(f"Error: {error}")

    ping_url = f"{CONFIG.get_healthchecks_protocol()}://{alert['ping_url']}"

    # create the linked cronjob
    cronjob_spec = _get_cronjob_spec(
        name, namespace, spec["timeout"], spec["node"], ping_url
    )
    kopf.adopt(cronjob_spec)
    cronjob = _create_cronjob(cronjob_spec, namespace)

    # set the healthcheck_id, ping_url, and cronjob uuid
    patch_object(
        name,
        namespace,
        {
            "healthcheck_id": alert["ping_url"].split("/")[-1],
            "ping_url": ping_url,
            "cronjob_uid": cronjob.metadata.uid,
            "cronjob_name": cronjob.metadata.name,
        },
        logger=logger,
    )

    return "Success"


def _update_nhc(body, spec, name, namespace, logger) -> str:
    """Update's a node health check"""
    try:
        alert = HCIO.update_alert(body["healthcheck_id"], spec_to_alert(spec))
    except Exception as error:
        raise kopf.PermanentError(f"Error: {error}")

    ping_url = f"{CONFIG.get_healthchecks_protocol()}://{alert['ping_url']}"

    # update the
    cronjob_spec = _get_cronjob_spec(
        name, namespace, spec["timeout"], spec["node"], ping_url
    )
    kopf.adopt(cronjob_spec)
    cronjob = _update_cronjob(cronjob_spec, namespace, body["cronjob_name"])
    patch_object(
        name,
        namespace,
        {
            "healthcheck_id": alert["ping_url"].split("/")[-1],
            "ping_url": ping_url,
            "cronjob_uid": cronjob.metadata.uid,
            "cronjob_name": cronjob.metadata.name,
        },
        logger=logger,
    )

    return "Updated"


# Startup logic
@kopf.on.startup()
async def startup_fn(**_):
    try:
        # kubernetes.config.load_incluster_config()
        config.load_kube_config()
    except Exception as e:
        raise kopf.PermanentError(f"Exception with K8s client setup: {e}")


@kopf.on.create(*CRD)
def create_fn(name: str, spec, logger, namespace: str, **_):
    return _create_nhc(name, namespace, logger, spec)


@kopf.on.delete(*CRD)
def delete_fn(body, **_):
    try:
        HCIO.delete_alert(body["healthcheck_id"])
    except Exception as error:
        raise kopf.PermanentError(f"Error: {error}")

    return "Deleted"


@kopf.on.update(*CRD)
def update_fn(body, spec, name: str, namespace: str, logger, **_):
    return _update_nhc(body, spec, name, namespace, logger)


@kopf.timer(*CRD, interval=CONFIG.interval_timer, idle=CONFIG.idle_timer)
def sync_monitor(body, spec, logger, name, namespace, **_) -> str:
    if not CONFIG.sync_checks:
        return ""

    uuid = body.get("healthcheck_id", None)
    if not uuid:
        logger.warn(
            "Tried to update check for %s in %s but no id in the object. Defaulting to create",
            name,
            namespace,
        )
        return _create_nhc(name, namespace, logger, spec)

    return _update_nhc(body, spec, name, namespace, logger)
