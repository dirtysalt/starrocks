// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.credential.azure;

import com.google.common.base.Preconditions;
import com.staros.proto.ADLS2CredentialInfo;
import com.staros.proto.ADLS2FileStoreInfo;
import com.staros.proto.AzBlobCredentialInfo;
import com.staros.proto.AzBlobFileStoreInfo;
import com.staros.proto.FileStoreInfo;
import com.staros.proto.FileStoreType;
import com.starrocks.common.Config;
import com.starrocks.connector.share.credential.CloudConfigurationConstants;
import com.starrocks.credential.CloudCredential;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.adl.AdlConfKeys;
import org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys;
import org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider;
import org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

// For Azure Blob Storage (wasb:// & wasbs://)
// We support Shared Key & SAS Token
// For Azure Data Lake Gen1 (adl://)
// We support Managed Service Identity & Service Principal
// For Azure Data Lake Gen2 (abfs:// & abfss://)
// We support Managed Identity & Shared Key & Service Principal
abstract class AzureStorageCloudCredential implements CloudCredential {

    public static final Logger LOG = LogManager.getLogger(AzureStorageCloudCredential.class);

    protected Map<String, String> generatedConfigurationMap = new HashMap<>();

    @Override
    public void applyToConfiguration(Configuration configuration) {
        for (Map.Entry<String, String> entry : generatedConfigurationMap.entrySet()) {
            configuration.set(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public boolean validate() {
        return !generatedConfigurationMap.isEmpty();
    }

    @Override
    public void toThrift(Map<String, String> properties) {
        properties.putAll(generatedConfigurationMap);
    }

    abstract void tryGenerateConfigurationMap();
}

class AzureBlobCloudCredential extends AzureStorageCloudCredential {
    private final String endpoint;
    private final String storageAccount;
    private final String sharedKey;
    private final String container;
    private final String sasToken;
    private final boolean useManagedIdentity;
    private final String clientId;
    private final String clientSecret;
    private final String tenantId;

    AzureBlobCloudCredential(String endpoint, String storageAccount, String sharedKey, String container, String sasToken,
                             boolean useManagedIdentity, String clientId, String clientSecret, String tenantId) {
        Preconditions.checkNotNull(endpoint);
        Preconditions.checkNotNull(storageAccount);
        Preconditions.checkNotNull(sharedKey);
        Preconditions.checkNotNull(container);
        Preconditions.checkNotNull(sasToken);
        Preconditions.checkNotNull(clientId);
        Preconditions.checkNotNull(clientSecret);
        Preconditions.checkNotNull(tenantId);
        this.endpoint = endpoint;
        this.storageAccount = storageAccount;
        this.sharedKey = sharedKey;
        this.container = container;
        this.sasToken = sasToken;
        this.useManagedIdentity = useManagedIdentity;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.tenantId = tenantId;
        tryGenerateConfigurationMap();
    }

    @Override
    void tryGenerateConfigurationMap() {
        if (!endpoint.isEmpty()) {
            // If user specific endpoint, they don't need to specific storage account anymore
            // Like if user is using Azurite, they need to specific endpoint
            if (!sharedKey.isEmpty()) {
                String key = String.format("fs.azure.account.key.%s", endpoint);
                generatedConfigurationMap.put(key, sharedKey);
            } else if (!container.isEmpty() && !sasToken.isEmpty()) {
                String key = String.format("fs.azure.sas.%s.%s", container, endpoint);
                generatedConfigurationMap.put(key, sasToken);
            }
        } else {
            if (!storageAccount.isEmpty() && !sharedKey.isEmpty()) {
                String key = String.format("fs.azure.account.key.%s.blob.core.windows.net", storageAccount);
                generatedConfigurationMap.put(key, sharedKey);
            } else if (!storageAccount.isEmpty() && !container.isEmpty() && !sasToken.isEmpty()) {
                String key =
                        String.format("fs.azure.sas.%s.%s.blob.core.windows.net", container, storageAccount);
                generatedConfigurationMap.put(key, sasToken);
            }
        }

        // For azure native sdk
        if (Config.azure_use_native_sdk) {
            if (!sharedKey.isEmpty()) {
                // shared key
                generatedConfigurationMap.put(CloudConfigurationConstants.AZURE_BLOB_SHARED_KEY, sharedKey);
            } else if (!sasToken.isEmpty()) {
                // sas token
                generatedConfigurationMap.put(CloudConfigurationConstants.AZURE_BLOB_SAS_TOKEN, sasToken);
            } else if (useManagedIdentity && !clientId.isEmpty()) {
                // user assigned managed identity
                generatedConfigurationMap.put(CloudConfigurationConstants.AZURE_BLOB_OAUTH2_CLIENT_ID, clientId);
            } else if (!clientId.isEmpty() && !clientSecret.isEmpty() && !tenantId.isEmpty()) {
                // client secret service principal
                generatedConfigurationMap.put(CloudConfigurationConstants.AZURE_BLOB_OAUTH2_CLIENT_ID, clientId);
                generatedConfigurationMap.put(CloudConfigurationConstants.AZURE_BLOB_OAUTH2_CLIENT_SECRET, clientSecret);
                generatedConfigurationMap.put(CloudConfigurationConstants.AZURE_BLOB_OAUTH2_TENANT_ID, tenantId);
            }
        }
    }

    @Override
    public String toCredString() {
        return "AzureBlobCloudCredential{" +
                "endpoint='" + endpoint + '\'' +
                ", storageAccount='" + storageAccount + '\'' +
                ", sharedKey='" + sharedKey + '\'' +
                ", container='" + container + '\'' +
                ", sasToken='" + sasToken + '\'' +
                ", useManagedIdentity='" + useManagedIdentity + '\'' +
                ", clientId='" + clientId + '\'' +
                ", clientSecret='" + clientSecret + '\'' +
                ", tenantId='" + tenantId + '\'' +
                '}';
    }

    @Override
    public FileStoreInfo toFileStoreInfo() {
        FileStoreInfo.Builder fileStore = FileStoreInfo.newBuilder();
        fileStore.setFsType(FileStoreType.AZBLOB);
        AzBlobFileStoreInfo.Builder azBlobFileStoreInfo = AzBlobFileStoreInfo.newBuilder();
        azBlobFileStoreInfo.setEndpoint(endpoint);
        AzBlobCredentialInfo.Builder azBlobCredentialInfo = AzBlobCredentialInfo.newBuilder();
        azBlobCredentialInfo.setSharedKey(sharedKey);
        azBlobCredentialInfo.setSasToken(sasToken);
        azBlobFileStoreInfo.setCredential(azBlobCredentialInfo.build());
        fileStore.setAzblobFsInfo(azBlobFileStoreInfo.build());
        return fileStore.build();
    }
}

class AzureADLS1CloudCredential extends AzureStorageCloudCredential {
    private final boolean useManagedServiceIdentity;
    private final String oauth2ClientId;
    private final String oauth2Credential;
    private final String oauth2Endpoint;

    public AzureADLS1CloudCredential(boolean useManagedServiceIdentity, String oauth2ClientId, String oauth2Credential,
                      String oauth2Endpoint) {
        Preconditions.checkNotNull(oauth2ClientId);
        Preconditions.checkNotNull(oauth2Credential);
        Preconditions.checkNotNull(oauth2Endpoint);
        this.useManagedServiceIdentity = useManagedServiceIdentity;
        this.oauth2ClientId = oauth2ClientId;
        this.oauth2Credential = oauth2Credential;
        this.oauth2Endpoint = oauth2Endpoint;

        tryGenerateConfigurationMap();
    }

    @Override
    void tryGenerateConfigurationMap() {
        if (useManagedServiceIdentity) {
            generatedConfigurationMap.put(AdlConfKeys.AZURE_AD_TOKEN_PROVIDER_TYPE_KEY, "Msi");
        } else if (!oauth2ClientId.isEmpty() && !oauth2Credential.isEmpty() &&
                !oauth2Endpoint.isEmpty()) {
            generatedConfigurationMap.put(AdlConfKeys.AZURE_AD_TOKEN_PROVIDER_TYPE_KEY, "ClientCredential");
            generatedConfigurationMap.put(AdlConfKeys.AZURE_AD_CLIENT_ID_KEY, oauth2ClientId);
            generatedConfigurationMap.put(AdlConfKeys.AZURE_AD_CLIENT_SECRET_KEY, oauth2Credential);
            generatedConfigurationMap.put(AdlConfKeys.AZURE_AD_REFRESH_URL_KEY, oauth2Endpoint);
        }
    }

    @Override
    public String toCredString() {
        return "AzureADLS1CloudCredential{" +
                "useManagedServiceIdentity=" + useManagedServiceIdentity +
                ", oauth2ClientId='" + oauth2ClientId + '\'' +
                ", oauth2Credential='" + oauth2Credential + '\'' +
                ", oauth2Endpoint='" + oauth2Endpoint + '\'' +
                '}';
    }

    @Override
    public FileStoreInfo toFileStoreInfo() {
        return null;
    }
}

class AzureADLS2CloudCredential extends AzureStorageCloudCredential {
    private final String endpoint;
    private final boolean oauth2ManagedIdentity;
    private final String oauth2TenantId;
    private final String oauth2ClientId;
    private final String storageAccount;
    private final String sharedKey;
    private final String sasToken;
    private final String oauth2ClientSecret;
    private final String oauth2ClientEndpoint;

    public AzureADLS2CloudCredential(String endpoint, boolean oauth2ManagedIdentity, String oauth2TenantId, String oauth2ClientId,
                                     String storageAccount, String sharedKey, String sasToken, String oauth2ClientSecret,
                                     String oauth2ClientEndpoint) {
        Preconditions.checkNotNull(endpoint);
        Preconditions.checkNotNull(oauth2TenantId);
        Preconditions.checkNotNull(oauth2ClientId);
        Preconditions.checkNotNull(storageAccount);
        Preconditions.checkNotNull(sharedKey);
        Preconditions.checkNotNull(sasToken);
        Preconditions.checkNotNull(oauth2ClientSecret);
        Preconditions.checkNotNull(oauth2ClientEndpoint);

        this.endpoint = endpoint;
        this.oauth2ManagedIdentity = oauth2ManagedIdentity;
        this.oauth2TenantId = oauth2TenantId;
        this.oauth2ClientId = oauth2ClientId;
        this.storageAccount = storageAccount;
        this.sharedKey = sharedKey;
        this.sasToken = sasToken;
        this.oauth2ClientSecret = oauth2ClientSecret;
        this.oauth2ClientEndpoint = oauth2ClientEndpoint;

        tryGenerateConfigurationMap();
    }

    @Override
    void tryGenerateConfigurationMap() {
        if (oauth2ManagedIdentity && !oauth2TenantId.isEmpty() && !oauth2ClientId.isEmpty()) {
            generatedConfigurationMap.put(createConfigKey(ConfigurationKeys.FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME),
                    "OAuth");
            generatedConfigurationMap.put(
                    createConfigKey(ConfigurationKeys.FS_AZURE_ACCOUNT_TOKEN_PROVIDER_TYPE_PROPERTY_NAME),
                    MsiTokenProvider.class.getName());
            generatedConfigurationMap.put(createConfigKey(ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_MSI_TENANT),
                    oauth2TenantId);
            generatedConfigurationMap.put(createConfigKey(ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_CLIENT_ID),
                    oauth2ClientId);
        } else if (!sharedKey.isEmpty()) {
            // Shared Key is always used by specific storage account, so we don't need to invoke createConfigKey()
            if (!storageAccount.isEmpty()) {
                generatedConfigurationMap.put(
                        String.format("fs.azure.account.auth.type.%s.dfs.core.windows.net", storageAccount),
                        "SharedKey");
                generatedConfigurationMap.put(
                        String.format("fs.azure.account.key.%s.dfs.core.windows.net", storageAccount),
                        sharedKey);
            } else if (!endpoint.isEmpty()) {
                generatedConfigurationMap.put(
                        String.format("fs.azure.account.auth.type.%s", endpoint),
                        "SharedKey");
                generatedConfigurationMap.put(
                        String.format("fs.azure.account.key.%s", endpoint),
                        sharedKey);
            }
        } else if (!sasToken.isEmpty()) {
            if (!storageAccount.isEmpty()) {
                generatedConfigurationMap.put(
                        String.format("fs.azure.account.auth.type.%s.dfs.core.windows.net", storageAccount),
                        "SAS");
                generatedConfigurationMap.put(
                        String.format("fs.azure.sas.fixed.token.%s.dfs.core.windows.net", storageAccount),
                        sasToken);
            } else if (!endpoint.isEmpty()) {
                generatedConfigurationMap.put(
                        String.format("fs.azure.account.auth.type.%s", endpoint),
                        "SAS");
                generatedConfigurationMap.put(
                        String.format("fs.azure.sas.fixed.token.%s", endpoint),
                        sasToken);
            }
        } else if (!oauth2ClientId.isEmpty() && !oauth2ClientSecret.isEmpty() &&
                !oauth2ClientEndpoint.isEmpty()) {
            generatedConfigurationMap.put(createConfigKey(ConfigurationKeys.FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME),
                    "OAuth");
            generatedConfigurationMap.put(
                    createConfigKey(ConfigurationKeys.FS_AZURE_ACCOUNT_TOKEN_PROVIDER_TYPE_PROPERTY_NAME),
                    ClientCredsTokenProvider.class.getName());
            generatedConfigurationMap.put(createConfigKey(ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_CLIENT_ID),
                    oauth2ClientId);
            generatedConfigurationMap.put(createConfigKey(ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_CLIENT_SECRET),
                    oauth2ClientSecret);
            generatedConfigurationMap.put(createConfigKey(ConfigurationKeys.FS_AZURE_ACCOUNT_OAUTH_CLIENT_ENDPOINT),
                    oauth2ClientEndpoint);
        }
    }

    @Override
    public String toCredString() {
        return "AzureADLS2CloudCredential{" +
                "oauth2ManagedIdentity=" + oauth2ManagedIdentity +
                ", oauth2TenantId='" + oauth2TenantId + '\'' +
                ", oauth2ClientId='" + oauth2ClientId + '\'' +
                ", endpoint='" + endpoint + '\'' +
                ", storageAccount='" + storageAccount + '\'' +
                ", sharedKey='" + sharedKey + '\'' +
                ", sasToken='" + sasToken + '\'' +
                ", oauth2ClientSecret='" + oauth2ClientSecret + '\'' +
                ", oauth2ClientEndpoint='" + oauth2ClientEndpoint + '\'' +
                '}';
    }

    @Override
    public FileStoreInfo toFileStoreInfo() {
        FileStoreInfo.Builder fileStore = FileStoreInfo.newBuilder();
        fileStore.setFsType(FileStoreType.ADLS2);
        ADLS2FileStoreInfo.Builder adls2FileStoreInfo = ADLS2FileStoreInfo.newBuilder();
        adls2FileStoreInfo.setEndpoint(endpoint);
        ADLS2CredentialInfo.Builder adls2CredentialInfo = ADLS2CredentialInfo.newBuilder();
        adls2CredentialInfo.setSharedKey(sharedKey);
        adls2CredentialInfo.setSasToken(sasToken);
        adls2CredentialInfo.setTenantId(oauth2TenantId);
        adls2CredentialInfo.setClientId(oauth2ClientId);
        adls2CredentialInfo.setClientSecret(oauth2ClientSecret);
        adls2CredentialInfo.setAuthorityHost(oauth2ClientEndpoint);
        adls2FileStoreInfo.setCredential(adls2CredentialInfo.build());
        fileStore.setAdls2FsInfo(adls2FileStoreInfo.build());
        return fileStore.build();
    }

    // Create Hadoop configuration key for specific storage account, if storage account is not set, means this property
    // is shared by all storage account.
    // This grammar only supported by ABFS
    // https://hadoop.apache.org/docs/r3.3.4/hadoop-azure/abfs.html#Configuring_ABFS
    private String createConfigKey(String key) {
        if (storageAccount.isEmpty()) {
            return key;
        } else {
            return String.format("%s.%s.dfs.core.windows.net", key, storageAccount);
        }
    }
}