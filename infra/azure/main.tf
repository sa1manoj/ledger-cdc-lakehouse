# Optional: ADLS Gen2 storage for running the lake on Azure instead of ./lake.
# terraform init && terraform apply -var="prefix=<unique-lowercase-name>"
# Remember `terraform destroy` when you are done to avoid charges.

terraform {
  required_version = ">= 1.6"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
  }
}

provider "azurerm" {
  features {}
}

variable "prefix" {
  description = "Lowercase letters/numbers, used to name resources (storage names are global)."
  type        = string
}

variable "location" {
  type    = string
  default = "eastus"
}

resource "azurerm_resource_group" "ledger" {
  name     = "${var.prefix}-ledger-rg"
  location = var.location
}

resource "azurerm_storage_account" "lake" {
  name                            = "${var.prefix}ledgerlake"
  resource_group_name             = azurerm_resource_group.ledger.name
  location                        = azurerm_resource_group.ledger.location
  account_tier                    = "Standard"
  account_replication_type        = "LRS"
  account_kind                    = "StorageV2"
  is_hns_enabled                  = true # hierarchical namespace = ADLS Gen2
  min_tls_version                 = "TLS1_2"
  allow_nested_items_to_be_public = false
}

resource "azurerm_storage_container" "lake" {
  name                  = "lake"
  storage_account_id    = azurerm_storage_account.lake.id
  container_access_type = "private"
}

output "lake_root" {
  value = "abfss://${azurerm_storage_container.lake.name}@${azurerm_storage_account.lake.name}.dfs.core.windows.net"
}

output "storage_account" {
  value = azurerm_storage_account.lake.name
}
