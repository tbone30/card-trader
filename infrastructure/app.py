import aws_cdk as cdk
from constructs import Construct
from aws_cdk import (
    aws_lambda as lambda_,
    aws_apigateway as apigateway,
    aws_dynamodb as dynamodb,
    aws_s3 as s3,
    aws_events as events,
    aws_events_targets as targets,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_sqs as sqs,
    aws_sns as sns,
    aws_iam as iam,
    aws_secretsmanager as secretsmanager,
    aws_logs as logs,
    Duration,
    RemovalPolicy
)
import json

class CardArbitrageStack(cdk.Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
        # Create secrets for API credentials
        self.create_secrets()
        
        # DynamoDB Tables - Pay per request, no VPC needed
        self.create_dynamodb_tables()
        
        # S3 and CloudFront for frontend
        self.create_frontend_infrastructure()
        
        # SQS and SNS for messaging
        self.create_messaging_infrastructure()
        
        # Lambda layers and functions
        self.create_lambda_infrastructure()
        
        # API Gateway
        self.create_api_gateway()
        
        # Step Functions workflow
        self.create_step_functions()
        
        # EventBridge rules for scheduling
        self.create_event_rules()
        
        # Output important values
        self.create_outputs()
    
    def create_secrets(self):
        """Create AWS Secrets Manager entries for API credentials"""
        self.ebay_credentials = secretsmanager.Secret(self, "EbayCredentials",
            description="eBay API credentials",
            secret_name="card-arbitrage/ebay-credentials",
            generate_secret_string=secretsmanager.SecretStringGenerator(
                secret_string_template=json.dumps({
                    "client_id": "your-ebay-client-id",
                    "client_secret": "your-ebay-client-secret",
                    "sandbox": "true"
                }),
                generate_string_key="placeholder",
                exclude_characters='"\\/'
            )
        )
        
        # TCG Player API credentials - commented out as not available
        # self.tcg_credentials = secretsmanager.Secret(self, "TCGCredentials",
        #     description="TCG Player API credentials", 
        #     secret_name="card-arbitrage/tcg-credentials",
        #     generate_secret_string=secretsmanager.SecretStringGenerator(
        #         secret_string_template=json.dumps({
        #             "public_key": "your-tcg-public-key",
        #             "private_key": "your-tcg-private-key",
        #             "sandbox": "true"
        #         }),
        #         generate_string_key="placeholder",
        #         exclude_characters='"\\/'
        #     )
        # )
    
    def create_dynamodb_tables(self):
        """Create DynamoDB tables with proper configuration"""
        # Listings table
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
            point_in_time_recovery_specification=dynamodb.PointInTimeRecoverySpecification(
                point_in_time_recovery_enabled=True
            )
        )
        
        # Add GSIs to the listings table
        self.listings_table.add_global_secondary_index(
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
        
        self.listings_table.add_global_secondary_index(
            index_name="platform-index",
            partition_key=dynamodb.Attribute(
                name="platform", 
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="scraped_at", 
                type=dynamodb.AttributeType.STRING
            )
        )
        
        # Opportunities table
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
            point_in_time_recovery_specification=dynamodb.PointInTimeRecoverySpecification(
                point_in_time_recovery_enabled=True
            )
        )
        
        # Add GSIs to the opportunities table
        self.opportunities_table.add_global_secondary_index(
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
        
        self.opportunities_table.add_global_secondary_index(
            index_name="platform-pair-index",
            partition_key=dynamodb.Attribute(
                name="platform_pair", 
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="profit_amount", 
                type=dynamodb.AttributeType.NUMBER
            )
        )
    
    def create_frontend_infrastructure(self):
        """Create S3 bucket for frontend"""
        self.website_bucket = s3.Bucket(self, "WebsiteBucket",
            website_index_document="index.html",
            website_error_document="error.html",
            public_read_access=True,
            block_public_access=s3.BlockPublicAccess(
                block_public_policy=False,
                restrict_public_buckets=False
            ),
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True
        )
    
    def create_messaging_infrastructure(self):
        """Create SQS and SNS for messaging"""
        # Dead letter queue
        self.dlq = sqs.Queue(self, "DeadLetterQueue",
            retention_period=Duration.days(14)
        )
        
        # Main scraping queue
        self.scraping_queue = sqs.Queue(self, "ScrapingQueue",
            visibility_timeout=Duration.minutes(15),
            retention_period=Duration.days(1),
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=3,
                queue=self.dlq
            )
        )
        
        # SNS topic for notifications
        self.notification_topic = sns.Topic(self, "ArbitrageNotifications",
            display_name="Card Arbitrage Notifications"
        )
    
    def create_lambda_infrastructure(self):
        """Create Lambda layers and functions"""
        
        # Shared layer with common dependencies
        self.shared_layer = lambda_.LayerVersion(self, "SharedLayer",
            code=lambda_.Code.from_asset("layers/shared"),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_9, lambda_.Runtime.PYTHON_3_10],
            description="Shared utilities and dependencies"
        )
        
        # Common environment variables
        common_env = {
            'LISTINGS_TABLE_NAME': self.listings_table.table_name,
            'OPPORTUNITIES_TABLE_NAME': self.opportunities_table.table_name,
            'NOTIFICATION_TOPIC_ARN': self.notification_topic.topic_arn,
            'EBAY_CREDENTIALS_SECRET': self.ebay_credentials.secret_arn,
            # 'TCG_CREDENTIALS_SECRET': self.tcg_credentials.secret_arn,  # Commented out - no TCG API access
            'LOG_LEVEL': 'INFO'
        }
        
        # API Handler Lambda
        self.api_handler_lambda = lambda_.Function(self, "ApiHandler",
            runtime=lambda_.Runtime.PYTHON_3_9,
            handler="handler.lambda_handler",
            code=lambda_.Code.from_asset("lambda_functions/api_handler"),
            layers=[self.shared_layer],
            environment=common_env,
            timeout=Duration.seconds(30),
            memory_size=256,
            retry_attempts=2
        )
        
        # eBay Scraper Lambda
        self.ebay_scraper_lambda = lambda_.Function(self, "EbayScraper",
            runtime=lambda_.Runtime.PYTHON_3_9,
            handler="handler.lambda_handler",
            code=lambda_.Code.from_asset("lambda_functions/ebay_scraper"),
            layers=[self.shared_layer],
            environment=common_env,
            timeout=Duration.minutes(10),
            memory_size=1024,
            retry_attempts=2
        )
        
        # TCG Player Scraper Lambda - commented out as no API access
        # self.tcg_scraper_lambda = lambda_.Function(self, "TCGScraper",
        #     runtime=lambda_.Runtime.PYTHON_3_9,
        #     handler="handler.lambda_handler",
        #     code=lambda_.Code.from_asset("lambda_functions/tcg_scraper"),
        #     layers=[self.shared_layer],
        #     environment=common_env,
        #     timeout=Duration.minutes(10),
        #     memory_size=1024,
        #     retry_attempts=2,
        #     reserved_concurrent_executions=10
        # )
        
        # Arbitrage Detector Lambda
        self.arbitrage_detector_lambda = lambda_.Function(self, "ArbitrageDetector",
            runtime=lambda_.Runtime.PYTHON_3_9,
            handler="handler.lambda_handler", 
            code=lambda_.Code.from_asset("lambda_functions/arbitrage_detector"),
            layers=[self.shared_layer],
            environment={
                **common_env,
                'MIN_PROFIT_MARGIN': '0.15',
                'MAX_RISK_SCORE': '2.0',
                'MAX_OPPORTUNITIES_PER_CARD': '10'
            },
            timeout=Duration.minutes(15),
            memory_size=1536,
            retry_attempts=2
        )
        
        # Notification Lambda
        self.notification_lambda = lambda_.Function(self, "NotificationHandler",
            runtime=lambda_.Runtime.PYTHON_3_9,
            handler="handler.lambda_handler",
            code=lambda_.Code.from_asset("lambda_functions/notification"),
            layers=[self.shared_layer],
            environment=common_env,
            timeout=Duration.seconds(30),
            memory_size=256
        )
        
        # Scheduler Lambda for coordinating scraping
        self.scheduler_lambda = lambda_.Function(self, "SchedulerLambda",
            runtime=lambda_.Runtime.PYTHON_3_9,
            handler="handler.lambda_handler",
            code=lambda_.Code.from_asset("lambda_functions/scheduler"),
            layers=[self.shared_layer],
            environment=common_env,
            timeout=Duration.minutes(2),
            memory_size=256
        )
        
        self.grant_permissions()
    
    def grant_permissions(self):
        """Grant necessary permissions to Lambda functions"""
        
        # DynamoDB permissions
        self.listings_table.grant_read_write_data(self.api_handler_lambda)
        self.listings_table.grant_read_write_data(self.ebay_scraper_lambda)
        # self.listings_table.grant_read_write_data(self.tcg_scraper_lambda)  # Commented out - no TCG scraper
        self.listings_table.grant_read_data(self.arbitrage_detector_lambda)
        
        self.opportunities_table.grant_read_write_data(self.api_handler_lambda)
        self.opportunities_table.grant_write_data(self.arbitrage_detector_lambda)
        self.opportunities_table.grant_read_data(self.notification_lambda)
        
        # Secrets Manager permissions
        self.ebay_credentials.grant_read(self.ebay_scraper_lambda)
        # self.tcg_credentials.grant_read(self.tcg_scraper_lambda)  # Commented out - no TCG scraper
        
        # SNS permissions
        self.notification_topic.grant_publish(self.notification_lambda)
        
        # SQS permissions
        self.scraping_queue.grant_consume_messages(self.scheduler_lambda)
    
    def create_api_gateway(self):
        """Create API Gateway with proper error handling"""
        
        # CloudWatch logs for API Gateway
        api_log_group = logs.LogGroup(self, "ApiGatewayLogs",
            log_group_name="/aws/apigateway/card-arbitrage",
            retention=logs.RetentionDays.ONE_WEEK,
            removal_policy=RemovalPolicy.DESTROY
        )
        
        self.api = apigateway.RestApi(self, "CardArbitrageApi",
            rest_api_name="Card Arbitrage API",
            description="API for card arbitrage opportunities",
            deploy_options=apigateway.StageOptions(
                access_log_destination=apigateway.LogGroupLogDestination(api_log_group),
                access_log_format=apigateway.AccessLogFormat.json_with_standard_fields(
                    caller=True,
                    http_method=True,
                    ip=True,
                    protocol=True,
                    request_time=True,
                    resource_path=True,
                    response_length=True,
                    status=True,
                    user=True
                ),
                logging_level=apigateway.MethodLoggingLevel.INFO,
                data_trace_enabled=True,
                metrics_enabled=True
            ),
            default_cors_preflight_options=apigateway.CorsOptions(
                allow_origins=apigateway.Cors.ALL_ORIGINS,
                allow_methods=apigateway.Cors.ALL_METHODS,
                allow_headers=["Content-Type", "Authorization", "X-Requested-With"]
            ),
            cloud_watch_role=True
        )
        
        # Request/response models
        error_model = self.api.add_model("ErrorModel",
            content_type="application/json",
            model_name="ErrorResponse",
            schema=apigateway.JsonSchema(
                schema=apigateway.JsonSchemaVersion.DRAFT4,
                type=apigateway.JsonSchemaType.OBJECT,
                properties={
                    "error": apigateway.JsonSchema(type=apigateway.JsonSchemaType.STRING),
                    "message": apigateway.JsonSchema(type=apigateway.JsonSchemaType.STRING)
                }
            )
        )
        
        # Lambda integration with error handling
        lambda_integration = apigateway.LambdaIntegration(
            self.api_handler_lambda,
            request_templates={
                "application/json": '{"statusCode": "200"}'
            },
            integration_responses=[
                apigateway.IntegrationResponse(
                    status_code="200",
                    response_templates={
                        "application/json": ""
                    }
                ),
                apigateway.IntegrationResponse(
                    status_code="400",
                    selection_pattern=".*\\[BadRequest\\].*",
                    response_templates={
                        "application/json": '{"error": "Bad Request", "message": $input.path("$.errorMessage")}'
                    }
                ),
                apigateway.IntegrationResponse(
                    status_code="500",
                    selection_pattern=".*\\[InternalError\\].*",
                    response_templates={
                        "application/json": '{"error": "Internal Server Error"}'
                    }
                )
            ]
        )
        
        # API routes
        # Health check
        health_resource = self.api.root.add_resource("health")
        health_resource.add_method("GET", lambda_integration,
            method_responses=[
                apigateway.MethodResponse(status_code="200"),
                apigateway.MethodResponse(status_code="500", response_models={"application/json": error_model})
            ]
        )
        
        # Opportunities endpoint
        opportunities_resource = self.api.root.add_resource("opportunities")
        opportunities_resource.add_method("GET", lambda_integration,
            method_responses=[
                apigateway.MethodResponse(status_code="200"),
                apigateway.MethodResponse(status_code="500", response_models={"application/json": error_model})
            ]
        )
        
        # Search endpoint
        search_resource = self.api.root.add_resource("search")
        search_resource.add_method("POST", lambda_integration,
            method_responses=[
                apigateway.MethodResponse(status_code="202"),
                apigateway.MethodResponse(status_code="400", response_models={"application/json": error_model}),
                apigateway.MethodResponse(status_code="500", response_models={"application/json": error_model})
            ]
        )
    
    def create_step_functions(self):
        """Create Step Functions workflow with proper error handling"""
        
        # Individual task definitions
        scrape_ebay_task = tasks.LambdaInvoke(self, "ScrapeEbayTask",
            lambda_function=self.ebay_scraper_lambda,
            output_path="$.Payload",
            retry_on_service_exceptions=True
        )
        scrape_ebay_task.add_retry(
            errors=["States.ALL"],
            interval=Duration.seconds(30),
            max_attempts=3,
            backoff_rate=2.0
        )
        
        # TCG Scraper task - commented out as no API access
        # scrape_tcg_task = tasks.LambdaInvoke(self, "ScrapeTCGTask",
        #     lambda_function=self.tcg_scraper_lambda,
        #     output_path="$.Payload",
        #     retry_on_service_exceptions=True,
        #     retry=sfn.RetryProps(
        #         errors=["States.TaskFailed", "States.ALL"],
        #         interval=Duration.seconds(30),
        #         max_attempts=3,
        #         backoff_rate=2.0
        #     )
        # )
        
        detect_arbitrage_task = tasks.LambdaInvoke(self, "DetectArbitrageTask",
            lambda_function=self.arbitrage_detector_lambda,
            output_path="$.Payload",
            retry_on_service_exceptions=True
        )
        detect_arbitrage_task.add_retry(
            errors=["States.TaskFailed"],
            interval=Duration.seconds(30),
            max_attempts=2,
            backoff_rate=2.0
        )
        
        send_notifications_task = tasks.LambdaInvoke(self, "SendNotificationsTask",
            lambda_function=self.notification_lambda,
            output_path="$.Payload"
        )
        
        # Define the workflow - only eBay scraping for now since TCG API is not available
        # When TCG API access is available, uncomment the parallel execution:
        # parallel_scraping = sfn.Parallel(self, "ParallelScraping")\
        #     .branch(scrape_ebay_task)\
        #     .branch(scrape_tcg_task)
        
        # For now, just run eBay scraping directly
        parallel_scraping = scrape_ebay_task
        
        check_opportunities = sfn.Choice(self, "CheckOpportunities")\
            .when(
                sfn.Condition.number_greater_than("$.opportunities_found", 0),
                send_notifications_task
            )\
            .otherwise(sfn.Succeed(self, "NoOpportunities"))
        
        # Complete workflow
        workflow_definition = parallel_scraping\
            .next(detect_arbitrage_task)\
            .next(check_opportunities)
        
        # Create state machine
        self.arbitrage_state_machine = sfn.StateMachine(self, "ArbitrageStateMachine",
            state_machine_name="card-arbitrage-workflow",
            definition_body=sfn.DefinitionBody.from_chainable(workflow_definition),
            timeout=Duration.minutes(30),
            logs=sfn.LogOptions(
                destination=logs.LogGroup(self, "StepFunctionLogs",
                    log_group_name="/aws/stepfunctions/card-arbitrage",
                    retention=logs.RetentionDays.ONE_WEEK,
                    removal_policy=RemovalPolicy.DESTROY
                ),
                level=sfn.LogLevel.ALL
            )
        )
        
        # Grant Step Functions permission to invoke Lambdas
        self.arbitrage_state_machine.grant_start_execution(self.api_handler_lambda)
        self.arbitrage_state_machine.grant_start_execution(self.scheduler_lambda)
        
        # Update Lambda environment variables
        for lambda_func in [self.api_handler_lambda, self.scheduler_lambda]:
            lambda_func.add_environment("ARBITRAGE_STATE_MACHINE_ARN", 
                                       self.arbitrage_state_machine.state_machine_arn)
    
    def create_event_rules(self):
        """Create EventBridge rules for scheduled scraping - FIXED VERSION"""
        
        popular_cards = [
            "Black Lotus",
            "Charizard Base Set", 
            "Blue-Eyes White Dragon",
            "Pikachu VMAX",
            "Mox Ruby"
        ]
        
        # Create individual rules for each card to avoid conflicts
        for i, card in enumerate(popular_cards):
            # Stagger the execution times by 5 minutes each
            minute = (i * 5) % 60
            hour = 8 + (i * 5) // 60
            
            rule = events.Rule(self, f"DailyScraping{i}",
                rule_name=f"daily-scraping-{card.lower().replace(' ', '-').replace(':', '')}",
                schedule=events.Schedule.cron(
                    minute=str(minute),
                    hour=str(hour)
                ),
                description=f"Daily card price scraping for {card}"
            )
            
            # Each rule targets the Step Functions state machine
            rule.add_target(targets.SfnStateMachine(
                self.arbitrage_state_machine,
                input=events.RuleTargetInput.from_object({
                    "card_name": card,
                    "scheduled": True,
                    "timestamp": events.Schedule.cron().expression_string
                })
            ))
        
        # General hourly check for high-value opportunities
        hourly_rule = events.Rule(self, "HourlyOpportunityCheck",
            schedule=events.Schedule.cron(minute="0"),
            description="Hourly check for high-value arbitrage opportunities"
        )
        
        hourly_rule.add_target(targets.LambdaFunction(
            self.scheduler_lambda,
            event=events.RuleTargetInput.from_object({
                "type": "hourly_check",
                "check_existing_opportunities": True
            })
        ))
    
    def create_outputs(self):
        """Create CloudFormation outputs"""
        cdk.CfnOutput(self, "ApiEndpoint",
            value=self.api.url,
            description="API Gateway endpoint URL"
        )
        cdk.CfnOutput(self, "WebsiteBucketOutput", 
            value=self.website_bucket.bucket_name,
            description="S3 website bucket name"
        )
        cdk.CfnOutput(self, "WebsiteUrl",
            value=f"http://{self.website_bucket.bucket_website_domain_name}",
            description="Website URL"
        )
        cdk.CfnOutput(self, "ListingsTableName",
            value=self.listings_table.table_name,
            description="DynamoDB listings table name"
        )
        cdk.CfnOutput(self, "OpportunitiesTableName",
            value=self.opportunities_table.table_name,
            description="DynamoDB opportunities table name"
        )
        cdk.CfnOutput(self, "StateMachineArn",
            value=self.arbitrage_state_machine.state_machine_arn,
            description="Step Functions state machine ARN"
        )
        cdk.CfnOutput(self, "EbayCredentialsSecret",
            value=self.ebay_credentials.secret_name,
            description="eBay credentials secret name"
        )
        # TCG credentials output commented out - no API access
        # cdk.CfnOutput(self, "TCGCredentialsSecret",
        #     value=self.tcg_credentials.secret_name,
        #     description="TCG Player credentials secret name"
        # )

app = cdk.App()
CardArbitrageStack(app, "CardArbitrageStack", 
    env=cdk.Environment(
        account=app.node.try_get_context("account"),
        region=app.node.try_get_context("region") or "us-east-1"
    )
)
app.synth()