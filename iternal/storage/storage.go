package storage

type Repository interface {
	StoreOperation(walletID, amount string) error
}