"""
AI Insight Generation Module for Automated BI System
Uses LLM to generate plain English insights from business metrics
"""
import os
import json
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime

# Try to import OpenAI, provide fallback
try:
    import openai
    from openai import OpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    logging.warning("OpenAI package not installed. Using mock insights.")

logger = logging.getLogger(__name__)

class InsightGenerator:
    """Generates AI-powered business insights from metrics"""
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize insight generator
        
        Args:
            api_key: OpenAI API key (optional, can be set via env var)
        """
        self.api_key = api_key or os.getenv('OPENAI_API_KEY')
        self.use_real_llm = OPENAI_AVAILABLE and self.api_key
        
        if self.use_real_llm:
            try:
                self.client = OpenAI(api_key=self.api_key)
                # Using gpt-4o-mini - faster, cheaper, better than gpt-3.5-turbo
                self.model = "gpt-4o-mini"
                logger.info(f"✅ OpenAI API initialized with model: {self.model}")
            except Exception as e:
                logger.error(f"Failed to initialize OpenAI: {e}")
                self.use_real_llm = False
        else:
            logger.info("Using mock insights generator")
    
    def generate_insights(self, 
                         metrics: Dict[str, Any], 
                         dataset_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate comprehensive business insights from metrics
        
        Args:
            metrics: Calculated business metrics
            dataset_info: Information about the dataset
            
        Returns:
            Dictionary containing insights and recommendations
        """
        logger.info("Generating AI insights from metrics")
        
        try:
            if self.use_real_llm and self._validate_metrics_for_ai(metrics):
                insights = self._generate_with_openai(metrics, dataset_info)
            else:
                insights = self._generate_mock_insights(metrics, dataset_info)
            
            # Add metadata
            insights['generated_at'] = datetime.now().isoformat()
            insights['insight_count'] = len(insights.get('key_insights', []))
            insights['ai_enabled'] = self.use_real_llm
            
            return insights
            
        except Exception as e:
            logger.error(f"Failed to generate insights: {str(e)}", exc_info=True)
            return self._generate_fallback_insights(metrics)
    
    def _validate_metrics_for_ai(self, metrics: Dict[str, Any]) -> bool:
        """Check if metrics have enough data for meaningful AI analysis"""
        summary = metrics.get('summary', {})
        
        # Need at least some data
        if summary.get('total_transactions', 0) < 10:
            logger.warning("Insufficient data for AI insights")
            return False
        
        return True
    
    def _generate_with_openai(self, 
                            metrics: Dict[str, Any], 
                            dataset_info: Dict[str, Any]) -> Dict[str, Any]:
        """Generate insights using OpenAI GPT"""
        
        prompt = self._build_insight_prompt(metrics, dataset_info)
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": self._get_system_prompt()},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.7,
                max_tokens=1200,
                response_format={"type": "json_object"}
            )
            
            content = response.choices[0].message.content
            
            # Parse JSON response
            try:
                insights = json.loads(content)
                
                # Validate structure
                if not self._validate_insight_structure(insights):
                    logger.warning("AI returned malformed insights, using mock")
                    return self._generate_mock_insights(metrics, dataset_info)
                
                return insights
                
            except json.JSONDecodeError:
                logger.error("Failed to parse AI response as JSON")
                return self._generate_mock_insights(metrics, dataset_info)
            
        except Exception as e:
            logger.error(f"OpenAI API error: {str(e)}")
            return self._generate_mock_insights(metrics, dataset_info)
    
    def _build_insight_prompt(self, 
                            metrics: Dict[str, Any], 
                            dataset_info: Dict[str, Any]) -> str:
        """Build the prompt for the LLM with FIXED data access paths"""
        
        summary = metrics.get('summary', {})
        revenue = metrics.get('revenue', {})
        customer = metrics.get('customer', {})
        product = metrics.get('product', {})
        growth = metrics.get('growth', {})
        time_series = metrics.get('time_series', {})
        
        # SAFELY extract top product (FIXED: handles empty case)
        top_products_dict = product.get('top_products', {})
        top_products_list = list(top_products_dict.items())
        top_product_info = top_products_list[0] if top_products_list else ("N/A", {"description": "No data", "total_revenue": 0})
        top_product_name = top_product_info[1].get('description', 'Unknown') if top_product_info != "N/A" else "No data"
        top_product_revenue = top_product_info[1].get('total_revenue', 0) if top_product_info != "N/A" else 0
        
        # SAFELY extract best day (FIXED: correct path)
        seasonality = time_series.get('seasonality', {})
        best_day = seasonality.get('best_day', 'N/A')
        
        prompt = f"""
        Analyze the following business metrics and generate actionable insights.

        DATASET INFORMATION:
        - Time Period: {summary.get('date_range', {}).get('start', 'N/A')} to {summary.get('date_range', {}).get('end', 'N/A')}
        - Total Rows: {dataset_info.get('rows', 'N/A')}
        - Data Type: Retail transactions

        BUSINESS METRICS:

        SUMMARY METRICS:
        - Total Revenue: ${summary.get('total_revenue', 0):,.2f}
        - Total Transactions: {summary.get('total_transactions', 0):,}
        - Total Customers: {summary.get('total_customers', 0):,}
        - Average Transaction Value: ${summary.get('avg_transaction_value', 0):.2f}
        - Total Products: {summary.get('total_products', 0):,}

        REVENUE ANALYSIS:
        - Monthly Revenue Trend: {list(revenue.get('monthly_revenue', {}).items())[-3:] if revenue.get('monthly_revenue') else 'N/A'}
        - Best Performing Day: {best_day}
        - Revenue Distribution - Mean: ${revenue.get('revenue_distribution', {}).get('mean', 0):.2f}, Median: ${revenue.get('revenue_distribution', {}).get('median', 0):.2f}

        CUSTOMER ANALYSIS:
        - Total Customers: {customer.get('customer_count', 0):,}
        - One-time Customers: {customer.get('one_time_customers', 0):,} ({round(customer.get('one_time_customers', 0)/max(customer.get('customer_count', 1),1)*100,1)}%)
        - Average Customer Value: ${customer.get('avg_customer_value', 0):.2f}
        - Segment Distribution: {customer.get('segment_distribution', {})}
        - RFM Scores - Recency: {customer.get('rfm_summary', {}).get('avg_recency', 0):.1f} days, Frequency: {customer.get('rfm_summary', {}).get('avg_frequency', 0):.1f}, Monetary: ${customer.get('rfm_summary', {}).get('avg_monetary', 0):.2f}

        PRODUCT ANALYSIS:
        - Total Products: {product.get('total_products', 0):,}
        - Top Product: {top_product_name} (${top_product_revenue:,.2f})  # FIXED: Safe extraction
        - Average Product Price: ${product.get('avg_product_price', 0):.2f}

        GROWTH METRICS:
        - Month-over-Month Growth: {growth.get('revenue_mom', 'N/A')}%
        - Overall Trend: {growth.get('revenue_trend', 'stable')}

        Please provide insights in the following JSON format:
        {{
            "executive_summary": "Brief 2-3 sentence overview of business health",
            "key_insights": [
                {{
                    "title": "Insight title",
                    "description": "Detailed explanation",
                    "impact": "high/medium/low",
                    "category": "revenue/customer/product/operational",
                    "confidence": 0.85,
                    "recommendation": "Actionable recommendation"
                }}
            ],
            "growth_opportunities": [
                {{
                    "opportunity": "Description of opportunity",
                    "potential_impact": "Estimated impact",
                    "effort_required": "low/medium/high"
                }}
            ],
            "risk_warnings": [
                {{
                    "risk": "Description of risk",
                    "severity": "high/medium/low",
                    "mitigation": "How to mitigate"
                }}
            ],
            "top_recommendations": [
                "Action 1",
                "Action 2",
                "Action 3"
            ]
        }}

        Focus on:
        1. Revenue trends and growth opportunities
        2. Customer behavior and retention
        3. Product performance and optimization
        4. Specific, actionable recommendations
        5. Data-driven observations only

        Be specific and avoid generic advice. Use the actual numbers provided.
        """
        
        return prompt
    
    def _get_system_prompt(self) -> str:
        """Get the system prompt for the AI"""
        return """You are a senior business intelligence analyst with 10+ years of experience at top consulting firms.
        
        Your analysis style:
        - Concise but insightful
        - Data-driven, never speculative
        - Action-oriented with specific recommendations
        - Professional business language
        
        Always:
        - Use actual numbers from the provided data
        - Highlight both strengths and weaknesses
        - Provide concrete, implementable recommendations
        - Acknowledge data limitations when relevant
        
        Format your response as valid JSON only, no additional text.
        """
    
    def _validate_insight_structure(self, insights: Dict[str, Any]) -> bool:
        """Validate that insights have the expected structure"""
        required_fields = ['executive_summary', 'key_insights', 'top_recommendations']
        
        for field in required_fields:
            if field not in insights:
                logger.warning(f"Missing required field: {field}")
                return False
        
        if not isinstance(insights.get('key_insights'), list):
            return False
        
        if not isinstance(insights.get('top_recommendations'), list):
            return False
        
        return True
    
    def _generate_mock_insights(self, 
                              metrics: Dict[str, Any], 
                              dataset_info: Dict[str, Any]) -> Dict[str, Any]:
        """Generate intelligent mock insights when LLM is not available"""
        
        summary = metrics.get('summary', {})
        customer = metrics.get('customer', {})
        product = metrics.get('product', {})
        growth = metrics.get('growth', {})
        time_series = metrics.get('time_series', {})
        
        # SAFELY get top product (FIXED)
        top_products = product.get('top_products', {})
        top_product_list = list(top_products.values())
        top_product_revenue = top_product_list[0].get('total_revenue', 0) if top_product_list else 0
        top_product_name = top_product_list[0].get('description', 'Unknown') if top_product_list else 'No data'
        
        # SAFELY get best day (FIXED)
        seasonality = time_series.get('seasonality', {})
        best_day = seasonality.get('best_day', 'N/A')
        
        # Determine business health
        health_scores = metrics.get('health_scores', {})
        overall_health = health_scores.get('overall_health', 50)
        
        if overall_health >= 70:
            health_status = "healthy"
        elif overall_health >= 40:
            health_status = "stable"
        else:
            health_status = "needs attention"
        
        # Generate executive summary
        exec_summary = f"""Business performance is {health_status} with ${summary.get('total_revenue', 0):,.2f} revenue from {summary.get('total_transactions', 0):,} transactions. 
        Customer base consists of {summary.get('total_customers', 0):,} customers with average transaction value of ${summary.get('avg_transaction_value', 0):.2f}.
        {growth.get('revenue_trend', 'Stable').capitalize()} revenue trend with {abs(growth.get('revenue_mom', 0)):.1f}% {'growth' if growth.get('revenue_mom', 0) > 0 else 'decline'} month-over-month.
        Best performing day is {best_day}."""
        
        # Build insights based on actual metrics
        key_insights = []
        
        # Revenue insight
        if summary.get('avg_transaction_value', 0) > 0:
            key_insights.append({
                "title": "Revenue Performance",
                "description": f"Total revenue of ${summary.get('total_revenue', 0):,.2f} from {summary.get('total_transactions', 0):,} transactions. Average order value is ${summary.get('avg_transaction_value', 0):.2f}. Best day: {best_day}.",
                "impact": "high" if summary.get('total_revenue', 0) > 10000 else "medium",
                "category": "revenue",
                "confidence": 0.95,
                "recommendation": "Implement upsell strategies and bundle products to increase average order value."
            })
        
        # Customer insight
        if customer.get('one_time_customers', 0) > 0 and customer.get('customer_count', 0) > 0:
            one_time_rate = (customer.get('one_time_customers', 0) / max(customer.get('customer_count', 1), 1)) * 100
            key_insights.append({
                "title": "Customer Retention",
                "description": f"{one_time_rate:.1f}% of customers are one-time buyers ({customer.get('one_time_customers', 0)} of {customer.get('customer_count', 0)}). Average customer lifetime value is ${customer.get('avg_customer_value', 0):.2f}.",
                "impact": "high" if one_time_rate > 60 else "medium",
                "category": "customer",
                "confidence": 0.9,
                "recommendation": "Develop a loyalty program and email re-engagement campaigns for one-time buyers."
            })
        
        # Product insight
        if product.get('total_products', 0) > 0 and top_product_revenue > 0:
            key_insights.append({
                "title": "Product Portfolio",
                "description": f"Active product catalog of {product.get('total_products', 0)} items. Top product '{top_product_name[:30]}...' generates ${top_product_revenue:,.2f} in revenue.",
                "impact": "medium",
                "category": "product",
                "confidence": 0.85,
                "recommendation": "Analyze bottom 20% of products for potential discontinuation or bundling opportunities."
            })
        
        # Growth insight
        if 'revenue_mom' in growth:
            trend = "positive" if growth['revenue_mom'] > 0 else "negative"
            key_insights.append({
                "title": "Growth Trajectory",
                "description": f"Month-over-month revenue growth is {growth['revenue_mom']:.1f}% ({trend} trend).",
                "impact": "high",
                "category": "revenue",
                "confidence": 0.9,
                "recommendation": f"Investigate drivers of {'growth' if growth['revenue_mom'] > 0 else 'decline'} and {'scale' if growth['revenue_mom'] > 0 else 'address'} accordingly."
            })
        
        # Generate growth opportunities
        growth_ops = []
        
        if customer.get('one_time_customers', 0) > customer.get('customer_count', 0) * 0.5:
            growth_ops.append({
                "opportunity": "Convert one-time buyers to repeat customers",
                "potential_impact": "15-25% revenue increase",
                "effort_required": "medium"
            })
        
        growth_ops.append({
            "opportunity": "Expand average order value through product bundling",
            "potential_impact": "10-15% revenue increase",
            "effort_required": "low"
        })
        
        if product.get('total_products', 0) < 50:
            growth_ops.append({
                "opportunity": "Expand product catalog in high-performing categories",
                "potential_impact": "20-30% revenue increase",
                "effort_required": "high"
            })
        
        # Generate risk warnings
        risks = []
        
        if customer.get('one_time_customers', 0) > customer.get('customer_count', 0) * 0.7:
            risks.append({
                "risk": "High customer churn risk",
                "severity": "high",
                "mitigation": "Implement customer retention program immediately"
            })
        
        if growth.get('revenue_mom', 0) < -10:
            risks.append({
                "risk": "Significant revenue decline",
                "severity": "high",
                "mitigation": "Conduct urgent business review and cost optimization"
            })
        
        # Check revenue concentration (FIXED: division by zero protection)
        total_revenue = summary.get('total_revenue', 0)
        if total_revenue > 0 and top_product_revenue > 0:
            concentration = (top_product_revenue / total_revenue) * 100
            if concentration > 40:
                risks.append({
                    "risk": f"High revenue concentration: Top product represents {concentration:.1f}% of revenue",
                    "severity": "medium",
                    "mitigation": "Diversify product portfolio and reduce dependency"
                })
        
        return {
            "executive_summary": exec_summary,
            "key_insights": key_insights,
            "growth_opportunities": growth_ops,
            "risk_warnings": risks,
            "top_recommendations": [
                "Implement customer loyalty program to increase retention",
                "Optimize top-performing products with targeted marketing",
                "Analyze slow-moving inventory for clearance opportunities"
            ][:3]
        }
    
    def _generate_fallback_insights(self, metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Generate minimal fallback insights when everything fails"""
        return {
            "executive_summary": "Basic analysis completed. Unable to generate AI-powered insights at this time.",
            "key_insights": [
                {
                    "title": "Data Analysis Complete",
                    "description": "Metrics have been calculated successfully. Configure OpenAI API key for automated insights.",
                    "impact": "medium",
                    "category": "operational",
                    "confidence": 1.0,
                    "recommendation": "Add OPENAI_API_KEY to environment variables for AI-powered insights."
                }
            ],
            "growth_opportunities": [],
            "risk_warnings": [],
            "top_recommendations": [
                "Configure OpenAI API key for automated insights",
                "Upload larger dataset for more comprehensive analysis",
                "Schedule regular data uploads for trend tracking"
            ],
            "generated_at": datetime.now().isoformat(),
            "note": "Mock insights generated - configure AI for real analysis"
        }

# Singleton Instance
insight_generator = InsightGenerator()