from datetime import *
from azure.identity import DefaultAzureCredential
from azure.mgmt.costmanagement import CostManagementClient
from azure.mgmt.costmanagement.models import (
    QueryColumnType,
    QueryGrouping,
    QueryDefinition,
    QueryOperatorType,
    ExportType,
    QueryAggregation,
    TimeframeType,
    QueryTimePeriod,
    QueryDataset,
    GranularityType,
    QueryGrouping,
    QueryColumnType,
    QueryFilter,
    QueryComparisonExpression,
)
import csv, time
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.subscription import SubscriptionClient
import azure.core.exceptions

# <---------------by using cost management sdk---------------->

credentials = DefaultAzureCredential()

def get_subscription_ids():

    '''Get Subscription Ids'''

    subscription_client = SubscriptionClient(credentials)
    subscription_ids = [subscription.subscription_id for subscription in subscription_client.subscriptions.list()]
    return subscription_ids

def get_resourceGroup_tag_list(subscription_id):

    '''Get ResourceGroup with tag list of Subscription Id'''

    resource_tag_list = []
    resource_client = ResourceManagementClient(credentials, subscription_id)

    resources = resource_client.resource_groups.list()
    for resource in resources:
        resource_tag_list.append({"name": resource.name, "tags": resource.tags})
    return resource_tag_list

def get_usage_cost(start_date, end_date):

    '''Get Cost and Usages of respective accounts'''

    cost_usages = []
    total_cost_usages = []

    client = CostManagementClient(  
        credential=credentials, base_url="https://management.azure.com"
    ) # Azure SDK to fetch data
    retry_delay = 5 # Delay after got an error
    count = 0

    # Get all Subscription Ids
    subscription_ids = get_subscription_ids()  
    
    # Itreate for each Id in Subscription
    for id in subscription_ids: 
        
        # Get ResourceGroup & Tags for for each Id
        resourceGroupsTags = get_resourceGroup_tag_list(id) 

        # Itreate for ResourceGroup
        for resourceGroup in resourceGroupsTags: 

            try:
                param = QueryDefinition(
                    type=ExportType.ACTUAL_COST,
                    timeframe=TimeframeType.CUSTOM,
                    time_period=QueryTimePeriod(from_property=start_date, to=end_date),
                    dataset=QueryDataset(
                        granularity=GranularityType.DAILY,
                        filter=QueryFilter(
                            dimensions=QueryComparisonExpression(
                                name="ResourceGroup",
                                operator=QueryOperatorType.IN,
                                values=[resourceGroup["name"]],
                            )
                        ),
                        aggregation={
                            "totalCost": QueryAggregation(name="Cost", function="SUM")
                        },
                        grouping=[
                            QueryGrouping(
                                type=QueryColumnType.DIMENSION, name="ResourceId"
                            ),
                            QueryGrouping(
                                type=QueryColumnType.DIMENSION, name="SubscriptionId"
                            ),
                            QueryGrouping(
                                type=QueryColumnType.DIMENSION, name="SubscriptionName"
                            ),
                            QueryGrouping(type=QueryColumnType.DIMENSION, name="ServiceName"),
                            QueryGrouping(type=QueryColumnType.DIMENSION, name="Meter"),
                            QueryGrouping(
                                type=QueryColumnType.DIMENSION, name="MeterCategory"
                            ),
                            QueryGrouping(
                                type=QueryColumnType.DIMENSION, name="MeterSubcategory"
                            ),
                            QueryGrouping(
                                type=QueryColumnType.DIMENSION, name="ResourceLocation"
                            ),
                            QueryGrouping(
                                type=QueryColumnType.DIMENSION, name="ChargeType"
                            ),
                        ]
                    ),
                )

                # Make a client to get usage and cost for subscription Id
                usage = client.query.usage(
                    scope=f"/subscriptions/{id}",
                    parameters=param,
                    content_type="application/json",
                )
                count += 1
                print(count)
                print(resourceGroup["name"])

                cost = 0
                if usage.rows != []:
                    for row in usage.rows:
                        cost_usages.append([
                            row[2], # ResourceId
                            (start_date).split("T")[0], # BillingPeriodStartDate
                            (end_date).split("T")[0], # BillingPeriodEndDate
                            row[1], # Date
                            row[3], # SubscriptionId
                            row[4], # SubscriptionName
                            row[6], # Meter
                            row[7], # MeterCategory
                            row[8], # MeterSubcategory
                            resourceGroup['name'], # ResourceGroupName
                            row[9], # ResourceLocation
                            row[5], # ServiceName
                            row[0], # CostInUsd
                            row[-1], # BillingCurrency
                            resourceGroup["tags"], # Tags
                            row[11], # ChargeType
                            ])
                        cost += row[0]
                    total_cost_usages.append(['', 
                                        (start_date).split("T")[0],
                                        (end_date).split("T")[0],
                                        '', '', '', '', '', '', '',
                                        f'Total for linked account# {row[2]} ({resourceGroup["name"]})',
                                        '',
                                        cost,
                                        row[-1],
                                        '',''])
                        
                else:
                    cost_usages.append([
                        None, 
                        (start_date).split("T")[0], 
                        (end_date).split("T")[0], 
                        None,
                        None, None, 
                        None, None, None, None, 
                        resourceGroup["name"],
                        None, 
                        None, 
                        0, 
                        None,
                        resourceGroup["tags"], 
                        None, None 
                    ])

            # Handle Rate limited & Service Unavailable error
            except azure.core.exceptions.HttpResponseError as e:
                status_code = e.status_code
                if status_code == 429:
                    print("Remaining number of requests for this specific client type", e.response.headers.get("x-ms-ratelimit-remaining-microsoft.costmanagement-clienttype-requests"))
                    retry_after = int(e.response.headers.get("x-ms-ratelimit-microsoft.costmanagement-entity-retry-after"))
                    print(f"Rate limited. Waiting for {retry_after} seconds...")
                    time.sleep(retry_after)
                elif status_code == 503:
                    retry_after = int(e.response.headers.get("Retry-After"))
                    print(f"Service Unavailable. Waiting for {retry_after} seconds...")
                    time.sleep(retry_after)
                else:
                    print(f"Error: {e}")
            except Exception as e:
                print(f"Error: {str(e)}")
                time.sleep(retry_delay)
           

            
    y, m, d = (start_date.split("T")[0]).split("-")
    get_month = datetime(int(y), int(m), int(d)).strftime("%B")
    csv_file_name = f"Azure_Billing_Data_{get_month}_{y}.csv"

    with open(csv_file_name, "w", newline="") as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(
            [
                "ResourceId",
                "billingPeriodStartDate",
                "BillingPeriodEndDate",
                "Date",
                "SubscriptionId",
                "SubscriptionName",
                "Meter",
                "MeterCategory",
                "MeterSubcategory",
                "ResourceGroupName",
                "ResourceLocation",
                "ServiceName",
                "CostInUsd",
                "BillingCurrency",
                "Tags",
                "ChargeType",
            ]
        )
        csv_writer.writerows(cost_usages)
        csv_writer.writerows(total_cost_usages)
    print("file downloaded successfully :)")

get_usage_cost(
    datetime(2023, 9, 1).strftime("%Y-%m-%dT%H:%M:%S+00:00"),
    datetime(2023, 9, 30).strftime("%Y-%m-%dT%H:%M:%S+00:00"),
)
