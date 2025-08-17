import aws_cdk as cdk
from constructs import Construct
from aws_cdk import (
    aws_lambda as lambda_,
    aws_apigateway as apigateway,
    aws_dynamodb as dynamodb,
    aws_s3 as s3,
    aws_cloudfront as cloudfront,
    aws_events as events,
    aws_stepfunctions as sfn,
    aws_sqs as sqs,
    aws_sns as sns,
    aws_iam as iam,
    Duration,
    RemovalPolicy
)
import json

class CardArbitrageStack(cdk.Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
        # DynamoDB Tables - Pay per request, no VPC needed
        self.listings_table = dynamodb.Table(self, "ListingsTable",
            table_name="card-listings",
            partition_key=dynamodb.Attribute(
                name="platform_card", 
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="item_id", 
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            time_to_live_attribute="ttl",
            removal_policy=RemovalPolicy.DESTROY,
            global_secondary_indexes=[
                dynamodb.GlobalSecondaryIndex(
                    index_name="card-name-index",
                    partition_key=dynamodb.Attribute(
                        name="card_name", 
                        type=dynamodb.AttributeType.STRING
                    ),
                    sort_key=dynamodb.Attribute(
                        name="price", 
                        type=dynamodb.AttributeType.NUMBER
                    )
                )
            ]
        )
        
        self.opportunities_table = dynamodb.Table(self, "OpportunitiesTable",
            table_name="arbitrage-opportunities",
            partition_key=dynamodb.Attribute(
                name="card_name", 
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="created_at", 
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            time_to_live_attribute="ttl",
            removal_policy=RemovalPolicy.DESTROY,
            global_secondary_indexes=[
                dynamodb.GlobalSecondaryIndex(
                    index_name="profit-margin-index",
                    partition_key=dynamodb.Attribute(
                        name="status", 
                        type=dynamodb.AttributeType.STRING
                    ),
                    sort_key=dynamodb.Attribute(
                        name="profit_margin", 
                        type=dynamodb.AttributeType.NUMBER
                    )
                )
            ]
        )
        
        # S3 for static website
        self.website_bucket = s3.Bucket(self, "WebsiteBucket",
            website_index_document="index.html",
            public_read_access=True,
            block_public_access=s3.BlockPublicAccess(
                block_public_policy=False,
                restrict_public_buckets=False
            ),
            removal_policy=RemovalPolicy.DESTROY
        )
        
        # CloudFront distribution
        self.distribution = cloudfront.CloudFrontWebDistribution(self, "Distribution",
            origin_configs=[
                cloudfront.SourceConfiguration(
                    s3_origin_source=cloudfront.S3OriginConfig(
                        s3_bucket_source=self.website_bucket
                    ),
                    behaviors=[cloudfront.Behavior(is_default_behavior=True)]
                )
            ]
        )
        
        # SQS for job processing (optional - could use direct Lambda invocation)
        self.scraping_queue = sqs.Queue(self, "ScrapingQueue",
            visibility_timeout=Duration.minutes(15),
            message_retention_period=Duration.days(1)
        )
        
        # SNS for notifications
        self.notification_topic = sns.Topic(self, "ArbitrageNotifications")
        
        # Lambda layer for shared dependencies
        self.shared_layer = lambda_.LayerVersion(self, "SharedLayer",
            code=lambda_.Code.from_asset("layers/shared"),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_9]
        )
        
        self.create_lambda_functions()
        self.create_api_gateway()
        self.create_step_functions()
        self.create_event_rules()
    
    def create_lambda_functions(self):
        """Create Lambda functions with DynamoDB access"""
        
        # Common environment variables
        common_env = {
            'LISTINGS_TABLE_NAME': self.listings_table.table_name,
            'OPPORTUNITIES_TABLE_NAME': self.opportunities_table.table_name,
            'NOTIFICATION_TOPIC_ARN': self.notification_topic.topic_arn
        }
        
        # API Handler Lambda
        self.api_handler_lambda = lambda_.Function(self, "ApiHandler",
            runtime=lambda_.Runtime.PYTHON_3_9,
            handler="handler.lambda_handler",
            code=lambda_.Code.from_asset("lambda_functions/api_handler"),
            layers=[self.shared_layer],
            environment=common_env,
            timeout=Duration.seconds(30)
        )
        
        # eBay Scraper Lambda
        self.ebay_scraper_lambda = lambda_.Function(self, "EbayScraper",
            runtime=lambda_.Runtime.PYTHON_3_9,
            handler="handler.lambda_handler",
            code=lambda_.Code.from_asset("lambda_functions/ebay_scraper"),
            layers=[self.shared_layer],
            environment={
                **common_env,
                'EBAY_CLIENT_ID': 'your-ebay-client-id',
                'EBAY_CLIENT_SECRET': 'your-ebay-client-secret'
            },
            timeout=Duration.minutes(5),
            memory_size=512
        )
        
        # TCG Player Scraper Lambda
        self.tcg_scraper_lambda = lambda_.Function(self, "TCGScraper",
            runtime=lambda_.Runtime.PYTHON_3_9,
            handler="handler.lambda_handler",
            code=lambda_.Code.from_asset("lambda_functions/tcg_scraper"),
            layers=[self.shared_layer],
            environment=common_env,
            timeout=Duration.minutes(5),
            memory_size=512
        )
        
        # Arbitrage Detector Lambda
        self.arbitrage_detector_lambda = lambda_.Function(self, "ArbitrageDetector",
            runtime=lambda_.Runtime.PYTHON_3_9,
            handler="handler.lambda_handler",
            code=lambda_.Code.from_asset("lambda_functions/arbitrage_detector"),
            layers=[self.shared_layer],
            environment={
                **common_env,
                'MIN_PROFIT_MARGIN': '0.15',
                'MAX_RISK_SCORE': '2.0'
            },
            timeout=Duration.minutes(10),
            memory_size=1024
        )
        
        # Notification Lambda
        self.notification_lambda = lambda_.Function(self, "NotificationHandler",
            runtime=lambda_.Runtime.PYTHON_3_9,
            handler="handler.lambda_handler",
            code=lambda_.Code.from_asset("lambda_functions/notification"),
            layers=[self.shared_layer],
            environment=common_env,
            timeout=Duration.seconds(30)
        )
        
        # Grant DynamoDB permissions
        self.listings_table.grant_read_write_data(self.api_handler_lambda)
        self.listings_table.grant_read_write_data(self.ebay_scraper_lambda)
        self.listings_table.grant_read_write_data(self.tcg_scraper_lambda)
        self.listings_table.grant_read_data(self.arbitrage_detector_lambda)
        
        self.opportunities_table.grant_read_write_data(self.api_handler_lambda)
        self.opportunities_table.grant_write_data(self.arbitrage_detector_lambda)
        self.opportunities_table.grant_read_data(self.notification_lambda)
        
        # Grant SNS permissions
        self.notification_topic.grant_publish(self.notification_lambda)
    
    def create_api_gateway(self):
        """Create API Gateway"""
        self.api = apigateway.RestApi(self, "CardArbitrageApi",
            rest_api_name="Card Arbitrage API",
            description="API for card arbitrage opportunities",
            default_cors_preflight_options=apigateway.CorsOptions(
                allow_origins=apigateway.Cors.ALL_ORIGINS,
                allow_methods=apigateway.Cors.ALL_METHODS,
                allow_headers=["Content-Type", "Authorization"]
            )
        )
        
        # API routes
        opportunities_resource = self.api.root.add_resource("opportunities")
        opportunities_resource.add_method("GET", 
            apigateway.LambdaIntegration(self.api_handler_lambda)
        )
        
        search_resource = self.api.root.add_resource("search")
        search_resource.add_method("POST", 
            apigateway.LambdaIntegration(self.api_handler_lambda)
        )
    
    def create_step_functions(self):
        """Create simplified Step Functions workflow"""
        
        # Step Functions role
        step_functions_role = iam.Role(self, "StepFunctionsRole",
            assumed_by=iam.ServicePrincipal("states.amazonaws.com")
        )
        
        # Grant permissions to invoke Lambda functions
        self.ebay_scraper_lambda.grant_invoke(step_functions_role)
        self.tcg_scraper_lambda.grant_invoke(step_functions_role)
        self.arbitrage_detector_lambda.grant_invoke(step_functions_role)
        self.notification_lambda.grant_invoke(step_functions_role)
        
        # Simplified workflow definition
        workflow_definition = {
            "Comment": "Card Arbitrage Detection Workflow",
            "StartAt": "ParallelScraping",
            "States": {
                "ParallelScraping": {
                    "Type": "Parallel",
                    "Branches": [
                        {
                            "StartAt": "ScrapeEbay",
                            "States": {
                                "ScrapeEbay": {
                                    "Type": "Task",
                                    "Resource": self.ebay_scraper_lambda.function_arn,
                                    "Retry": [{
                                        "ErrorEquals": ["States.TaskFailed"],
                                        "IntervalSeconds": 30,
                                        "MaxAttempts": 2,
                                        "BackoffRate": 2.0
                                    }],
                                    "End": True
                                }
                            }
                        },
                        {
                            "StartAt": "ScrapeTCG", 
                            "States": {
                                "ScrapeTCG": {
                                    "Type": "Task",
                                    "Resource": self.tcg_scraper_lambda.function_arn,
                                    "Retry": [{
                                        "ErrorEquals": ["States.TaskFailed"],
                                        "IntervalSeconds": 30,
                                        "MaxAttempts": 2,
                                        "BackoffRate": 2.0
                                    }],
                                    "End": True
                                }
                            }
                        }
                    ],
                    "Next": "DetectArbitrage"
                },
                "DetectArbitrage": {
                    "Type": "Task",
                    "Resource": self.arbitrage_detector_lambda.function_arn,
                    "Next": "CheckOpportunities"
                },
                "CheckOpportunities": {
                    "Type": "Choice",
                    "Choices": [{
                        "Variable": "$.opportunities_found",
                        "NumericGreaterThan": 0,
                        "Next": "SendNotifications"
                    }],
                    "Default": "Complete"
                },
                "SendNotifications": {
                    "Type": "Task",
                    "Resource": self.notification_lambda.function_arn,
                    "End": True
                },
                "Complete": {
                    "Type": "Succeed"
                }
            }
        }
        
        # Create state machine
        self.arbitrage_state_machine = sfn.CfnStateMachine(self, "ArbitrageStateMachine",
            state_machine_name="card-arbitrage-workflow",
            definition_string=json.dumps(workflow_definition),
            role_arn=step_functions_role.role_arn
        )
        
        # Update Lambda environment with state machine ARN
        for lambda_func in [self.api_handler_lambda, self.arbitrage_detector_lambda]:
            lambda_func.add_environment("ARBITRAGE_STATE_MACHINE_ARN", 
                                       self.arbitrage_state_machine.attr_arn)
    
    def create_event_rules(self):
        """Create EventBridge rules for scheduled scraping"""
        
        # Daily scraping for popular cards
        daily_rule = events.Rule(self, "DailyScraping",
            schedule=events.Schedule.cron(hour="8", minute="0"),
            description="Daily card price scraping"
        )
        
        # Add popular cards list to scrape daily
        popular_cards = [
            "Black Lotus",
            "Charizard Base Set",
            "Blue-Eyes White Dragon"
        ]
        
        for card in popular_cards:
            daily_rule.add_target(events.targets.SfnStateMachine(
                self.arbitrage_state_machine,
                input=events.RuleTargetInput.from_object({
                    "card_name": card,
                    "scheduled": True
                })
            ))

# Output important values
        cdk.CfnOutput(self, "ApiEndpoint",
            value=self.api.url,
            description="API Gateway endpoint URL"
        )
        
        cdk.CfnOutput(self, "WebsiteUrl",
            value=f"https://{self.distribution.distribution_domain_name}",
            description="CloudFront website URL"
        )
        
        cdk.CfnOutput(self, "ListingsTableName",
            value=self.listings_table.table_name,
            description="DynamoDB listings table name"
        )

app = cdk.App()
CardArbitrageStack(app, "CardArbitrageStack")
app.synth()