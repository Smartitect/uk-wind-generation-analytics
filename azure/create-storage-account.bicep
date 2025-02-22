// This script deploys all of the Azure resources required to support the Titanic Demo

// See following locaiton for setting up SFTP:
// https://github.com/Azure/azure-quickstart-templates/blob/master/quickstarts/microsoft.storage/storage-sftp/README.md

// PARAMETERS --------------------------------------------------------------------------------------------------------------------
@minLength(1)
@maxLength(4)
@description('The descriptive prefix for the resources, maximum 4 characters long.')
param appNamePrefix string

@allowed([
  'dev'
  'tst'
  'prd'
])
@description('The environment type for the deployment, dev, tst or prd.')
param environmentType string

@description('The location for the resources, defaults to the location of the resource group.')
param location string = resourceGroup().location

@description('The suffix for the app name, defaults to a unique string based on the resource group id.')
param appNameSuffix string = uniqueString(resourceGroup().id)

@description('Name of the container.')
param containerNames array = ['bronze', 'silver', 'gold']

// VARIABLES --------------------------------------------------------------------------------------------------------------------
// Build names for resources from the parameters above.
var appName = '${appNamePrefix}-${environmentType}-${appNameSuffix}'
var logAnalyticsWorkspaceName = 'law-${appName}'
var appInsightsName = 'appi-${appName}'
var storageAccountName = 'stor${replace(appName, '-', '')}'
var keyVaultName = 'keyv${replace(appName, '-', '')}'

// STORAGE --------------------------------------------------------------------------------------------------------------------
// Storage account is set up to provide container for data.
resource storageAccount 'Microsoft.Storage/storageAccounts@2023-01-01' = {
  name: storageAccountName
  location: location
  sku: {
    name: 'Standard_LRS'
  }
  kind: 'StorageV2'
  properties: {
    isHnsEnabled: true
  }
}

resource blobStorage 'Microsoft.Storage/storageAccounts/blobServices@2021-04-01' = {
  parent: storageAccount
  name: 'default'
  properties: {}
}

// Create bronze, silver and gold containers.
resource blobContainers 'Microsoft.Storage/storageAccounts/blobServices/containers@2021-04-01' = [for containerName in containerNames: {
    parent: blobStorage
    name: containerName
    properties: {
        publicAccess: 'None'
    }
}]

// LOGGING --------------------------------------------------------------------------------------------------------------------
// Set up log analytics workspace and app insights to capture logs, metrics, telemetry and traces.
resource logAnalyticsWorkspace 'Microsoft.OperationalInsights/workspaces@2020-10-01' = {
  name: logAnalyticsWorkspaceName
  location: location
  properties: {
    sku: {
      name: 'PerGB2018'
    }
    retentionInDays: 30
    features: {
      enableLogAccessUsingOnlyResourcePermissions: true
    }
    // workspaceCapping: {
    //   dailyQuotaGb: 1
    // }
    publicNetworkAccessForIngestion: 'Enabled'
    publicNetworkAccessForQuery: 'Enabled'
  }
}

resource appInsights 'Microsoft.Insights/components@2020-02-02-preview' = {
  name: appInsightsName
  location: location
  kind: 'web'
  properties: {
    Application_Type: 'web'
    // Flow_Type: any('Redfield')
    // Request_Source: any('IbizaAIExtension')
    WorkspaceResourceId: logAnalyticsWorkspace.id
    IngestionMode: 'LogAnalytics'
    publicNetworkAccessForIngestion: 'Enabled'
    publicNetworkAccessForQuery: 'Enabled'
  }
}

// SECRETS --------------------------------------------------------------------------------------------------------------------
// Set up a key vault to store secrets.
resource keyVault 'Microsoft.KeyVault/vaults@2023-07-01' = {
  name: keyVaultName
  location: location
  properties: {
    accessPolicies:[]
    enableRbacAuthorization: true
    enableSoftDelete: true
    softDeleteRetentionInDays: 90
    enabledForDeployment: false
    enabledForDiskEncryption: false
    enabledForTemplateDeployment: true
    tenantId: subscription().tenantId
    sku: {
      name: 'standard'
      family: 'A'
    }
    networkAcls: {
      defaultAction: 'Allow'
      bypass: 'AzureServices'
    }
  }
}

resource secretAppInsights 'Microsoft.KeyVault/vaults/secrets@2019-09-01' = {
  parent: keyVault
  name: 'APPLICATION-INSIGHTS-CONNECTION-STRING'
  properties: {
    value: appInsights.properties.ConnectionString
  }
}

resource secretStorageAccountName 'Microsoft.KeyVault/vaults/secrets@2019-09-01' = {
    parent: keyVault
    name: 'STORAGE-ACCOUNT-NAME'
    properties: {
        value: storageAccount.name
    }
}

resource secretStorageAccountConnectionString 'Microsoft.KeyVault/vaults/secrets@2019-09-01' = {
    parent: keyVault
    name: 'STORAGE-ACCOUNT-CONNECTION-STRING'
    properties: {
        value: listKeys(storageAccount.id, storageAccount.apiVersion).keys[0].value
    }
}

output storageAccountName string = storageAccount.name
output keyVaultName string = keyVault.name

output nameSecretAppInsights string = secretAppInsights.name
output nameSecretStorageAccountName string = secretStorageAccountName.name
output nameSecretStorageAccountConnectionString string = secretStorageAccountConnectionString.name
