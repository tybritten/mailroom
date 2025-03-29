package ai

type ReasoningError struct {
	msg          string
	Instructions string
	Input        string
	Response     string
}

func NewReasoningError(msg string, instructions, input, response string) *ReasoningError {
	return &ReasoningError{
		msg:          msg,
		Instructions: instructions,
		Input:        input,
		Response:     response,
	}
}

func (e *ReasoningError) Error() string { return e.msg }
