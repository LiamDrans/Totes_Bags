resource "aws_cloudwatch_event_rule" "every_30_minutes" {
  name        = "trigger_lambda_every_30_minutes"
  description = "Triggers Lambda every 30 minutes"
  schedule_expression = "rate(30 minutes)"
}

resource "aws_cloudwatch_event_target" "lambda_target" {
  rule      = aws_cloudwatch_event_rule.every_30_minutes.name
  target_id = "task_extract"
  arn       = aws_lambda_function.task_extract.arn
}


resource "aws_lambda_permission" "allow_eventbridge" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.task_extract.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.every_30_minutes.arn
}