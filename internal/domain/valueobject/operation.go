package valueobject

// Operation represents the type of database operation
type Operation string

const (
	// Insert represents an insert operation
	Insert Operation = "INSERT"
	
	// Update represents an update operation
	Update Operation = "UPDATE"
	
	// Delete represents a delete operation
	Delete Operation = "DELETE"
)

// String returns the string representation of the operation
func (o Operation) String() string {
	return string(o)
}

// IsValid checks if the operation is valid
func (o Operation) IsValid() bool {
	switch o {
	case Insert, Update, Delete:
		return true
	default:
		return false
	}
}
