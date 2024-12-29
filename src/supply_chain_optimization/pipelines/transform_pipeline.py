from src.supply_chain_optimization.components.data_ingestion import DataIngestion
from src.supply_chain_optimization.components.data_ingestion import DataIngestionConfig
from src.supply_chain_optimization.components.data_transformation import DataTransformation
from src.supply_chain_optimization.exception import CustomException
from src.supply_chain_optimization.logger import logging
import sys
from dataclasses import dataclass

class transformer_Pipeline:
    def __init__(self):
        self.ingestion = DataIngestion()
        self.transformation = DataTransformation()

    def execute_pipelines(self):
        try:

            train_path, test_path = self.ingestion.initiate_data_ingestion()

            tasks = [
                {
                    "task": "late_delivery_risk_prediction",
                    "numerical_col": ["Days_for_shipping_real", "Days_for_shipment_scheduled", "Product_Price", "Order_Item_Quantity", "Sales"],
                    "categorical_col": ["Shipping_Mode"],
                    "target": "Late_delivery_risk",
                    "model_features": ["Category_Id", "Category_Name", "Days_for_shipping_real", "Days_for_shipment_scheduled", "Shipping_Mode", "Order_State", "Order_Region","Order_Country", "Product_Price", "Sales", "Order_Item_Quantity", "Order_Item_Discount_Rate"],
                },
                {
                    "task": "order_profit_prediction",
                    "numerical_col": ["Days_for_shipping_real", "Days_for_shipment_scheduled", "Product_Price", "Order_Item_Quantity", "Sales"],
                    "categorical_col": ["Shipping_Mode"],
                    "target": "Order_Profit_Per_Order",
                    "model_features": ["Category_Id", "Category_Name", "Days_for_shipping_real", "Days_for_shipment_scheduled", "Shipping_Mode", "Order_State", "Order_Region","Order_Country", "Product_Price", "Sales", "Order_Item_Quantity", "Order_Item_Discount_Rate"],
                },
                {
                    "task": "shipping_time_prediction",
                    "numerical_col": ["Days_for_shipment_scheduled", "Product_Price", "Order_Item_Quantity", "Sales"],
                    "categorical_col": ["Shipping_Mode"],
                    "target": "Days_for_shipping_real",
                    "model_features": ["Category_Id", "Category_Name", "Days_for_shipment_scheduled", "Shipping_Mode", "Order_State", "Order_Region","Order_Country", "Product_Price", "Sales", "Order_Item_Quantity", "Order_Item_Discount_Rate"],
                }
            ]

            for task in tasks:
                features_config = {
                    task["target"]: {
                        "numerical": task['numerical_col'],
                        "categorical": task['categorical_col'],
                        "features": task['model_features']
                    }
                }

                transformed_data = self.transformation.initiate_data_transformation(train_path, test_path, features_config)
                
        except Exception as e:
            raise CustomException(e, sys)
