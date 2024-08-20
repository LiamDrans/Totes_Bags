variable "extract_lambda" {
    type    = string
    default = "extract_sample"
}

variable "transform_lambda" {
    type    = string
    default = "transform_sample"
}

variable "load_lambda" {
    type    = string
    default = "load_sample"
}

variable "default_timeout" {
    type    = number
    default = 30
}

# can add a variable state machine name if need be