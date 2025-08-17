# infrastructure/app.py
import aws_cdk as cdk
from constructs import Construct
from aws_cdk import (
    aws_lambda as lambda_,
    aws_apigateway as apigateway,
    aws_rds as rds,
    aws_ec2 as ec2,
    aws_s3 as s3,
    aws_cloudfront as cloudfront,
    aws_events as events,
    aws_stepfunctions as sfn,
    aws_sqs as sqs,
    aws_sns as sns,
    aws_elasticache as elasticache,
    Duration
)

class CardArbitrageStack(cdk.Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
        # VPC for database and cache
        self.vpc = ec2.Vpc(self, "CardArbitrageVpc",
            max_azs=2,
            nat_gateways=1
        )
        
        # RDS Aurora Serverless
        self.database = rds.ServerlessCluster(self, "CardDatabase",
            engine=rds.DatabaseClusterEngine.aurora_postgres(
                version=rds.AuroraPostgresEngineVersion.VER_13_7
            ),
            vpc=self.vpc,
            scaling=rds.ServerlessScalingOptions(
                auto_pause=Duration.minutes(10),
                min_capacity=rds.AuroraCapacityUnit.ACU_2,
                max_capacity=rds.AuroraCapacityUnit.ACU_16
            ),
            default_database_name="carddb"
        )
        
        # ElastiCache Redis
        self.cache = elasticache.CfnCacheCluster(self, "RedisCache",
            cache_node_type="cache.t3.micro",
            engine="redis",
            num_cache_nodes=1,
            cache_subnet_group_name=self.create_cache_subnet_group()
        )
        
        # S3 for static website
        self.website_bucket = s3.Bucket(self, "WebsiteBucket",
            website_index_document="index.html",
            public_read_access=True,
            block_public_access=s3.BlockPublicAccess(
                block_public_policy=False,
                restrict_public_buckets=False
            )
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
        
        # SQS for job processing
        self.scraping_queue = sqs.Queue(self, "ScrapingQueue",
            visibility_timeout=Duration.minutes(15)
        )
        
        self.arbitrage_queue = sqs.Queue(self, "ArbitrageQueue",
            visibility_timeout=Duration.minutes(5)
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
    
    def create_cache_subnet_group(self):
        subnet_group = elasticache.CfnSubnetGroup(self, "CacheSubnetGroup",
            description="Subnet group for ElastiCache",
            subnet_ids=[subnet.subnet_id for subnet in self.vpc.private_subnets]
        )
        return subnet_group.ref
    
    # infrastructure/app.py
def create_step_functions(self):
    """Create Step Functions state machine for arbitrage workflow"""
    
    # Define the state machine definition
    arbitrage_workflow_definition = {
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
                                "Retry": [
                                    {
                                        "ErrorEquals": ["States.TaskFailed"],
                                        "IntervalSeconds": 30,
                                        "MaxAttempts": 3,
                                        "BackoffRate": 2.0
                                    }
                                ],
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
                                "Retry": [
                                    {
                                        "ErrorEquals": ["States.TaskFailed"],
                                        "IntervalSeconds": 30,
                                        "MaxAttempts": 3,
                                        "BackoffRate": 2.0
                                    }
                                ],
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
                "Choices": [
                    {
                        "Variable": "$.opportunities_found",
                        "NumericGreaterThan": 0,
                        "Next": "SendNotifications"
                    }
                ],
                "Default": "Complete"
            },
            "SendNotifications": {
                "Type": "Task",
                "Resource": self.notification_lambda.function_arn,
                "Next": "Complete"
            },
            "Complete": {
                "Type": "Succeed"
            }
        }
    }
    
    # Create IAM role for Step Functions
    step_functions_role = iam.Role(self, "StepFunctionsRole",
        assumed_by=iam.ServicePrincipal("states.amazonaws.com"),
        managed_policies=[
            iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSStepFunctionsFullAccess")
        ]
    )
    
    # Grant Step Functions permission to invoke Lambda functions
    self.ebay_scraper_lambda.grant_invoke(step_functions_role)
    self.tcg_scraper_lambda.grant_invoke(step_functions_role)
    self.arbitrage_detector_lambda.grant_invoke(step_functions_role)
    self.notification_lambda.grant_invoke(step_functions_role)
    
    # Create the state machine
    self.arbitrage_state_machine = sfn.CfnStateMachine(self, "ArbitrageStateMachine",
        state_machine_name="card-arbitrage-workflow",
        definition_string=json.dumps(arbitrage_workflow_definition),
        role_arn=step_functions_role.role_arn
    )
    
    # Output the state machine ARN for Lambda environment variables
    cdk.CfnOutput(self, "StateMachineArn",
        value=self.arbitrage_state_machine.attr_arn,
        description="ARN of the arbitrage detection state machine"
    )