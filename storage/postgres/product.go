package postgres

import (
	"database/sql"
	"fmt"
	"test/api/models"
	"test/storage"

	"github.com/google/uuid"
)

type productRepo struct {
	db *sql.DB
}

func NewProductRepo(db *sql.DB) storage.IProductStorage {
	return productRepo{db: db}
}

func (p productRepo) Create(product models.CreateProduct) (string, error) {
	id := uuid.New()
	query := `insert into products(id, name, price, original_price, quantity, category_id) 
						values($1, $2, $3, $4, $5, $6)`

	if _, err := p.db.Exec(query,
		id,
		product.Name,
		product.Price,
		product.OriginalPrice,
		product.Quantity,
		product.CategoryID); err != nil {
		fmt.Println("error is while inserting product", err.Error())
		return "", err
	}

	return id.String(), nil
}

func (p productRepo) GetByID(key models.PrimaryKey) (models.Product, error) {
	product := models.Product{}
	query := `select id, name, price, original_price, quantity, category_id from products where id = $1 `
	if err := p.db.QueryRow(query, key.ID).Scan(
		&product.ID,
		&product.Name,
		&product.Price,
		&product.OriginalPrice,
		&product.Quantity,
		&product.CategoryID); err != nil {
		fmt.Println("error is while selecting product by id", err.Error())
		return models.Product{}, err
	}
	return product, nil
}

func (p productRepo) GetList(request models.GetListRequest) (models.ProductResponse, error) {
	var (
		products          = []models.Product{}
		page              = request.Page
		offset            = (page - 1) * request.Limit
		search            = request.Search
		query, countQuery string
		count             = 0
	)

	countQuery = `SELECT COUNT(1) FROM products `

	if search != "" {
		countQuery += fmt.Sprintf(`WHERE (name ILIKE '%%%s%%' OR 
			CAST(price AS TEXT) ILIKE '%%%s%%' OR CAST(quantity AS TEXT) ILIKE '%%%s%%')`, search, search, search)
	}

	if err := p.db.QueryRow(countQuery).Scan(&count); err != nil {
		fmt.Println("error while scanning count", err.Error())
		return models.ProductResponse{}, err
	}

	query = `SELECT id, name, price, original_price, quantity, category_id FROM products `

	if search != "" {
		query += fmt.Sprintf(`WHERE (name ILIKE '%%%s%%' OR 
			CAST(price AS TEXT) ILIKE '%%%s%%' OR CAST(quantity AS TEXT) ILIKE '%%%s%%')`, search, search, search)
	}

	query += ` LIMIT $1 OFFSET $2`

	rows, err := p.db.Query(query, request.Limit, offset)
	if err != nil {
		fmt.Println("error while selecting products", err.Error())
		return models.ProductResponse{}, err
	}

	for rows.Next() {
		product := models.Product{}
		if err = rows.Scan(
			&product.ID,
			&product.Name,
			&product.Price,
			&product.OriginalPrice,
			&product.Quantity,
			&product.CategoryID); err != nil {
			fmt.Println("error while scanning products", err.Error())
			return models.ProductResponse{}, err
		}
		products = append(products, product)
	}
	return models.ProductResponse{
		Product: products,
		Count:   count,
	}, nil
}

func (p productRepo) Update(product models.UpdateProduct) (string, error) {
	query := `update products set name = $1, price = $2, original_price = $3, quantity = $4, category_id = $5 where id = $6`

	if _, err := p.db.Exec(query,
		&product.Name,
		&product.Price,
		&product.OriginalPrice,
		&product.Quantity,
		&product.CategoryID,
		&product.ID); err != nil {
		fmt.Println("error is while updating product", err.Error())
		return "", err
	}

	return product.ID, nil
}

func (p productRepo) Delete(key models.PrimaryKey) error {
	query := `delete from products where id = $1`

	if _, err := p.db.Exec(query, key.ID); err != nil {
		fmt.Println("error is while deleting product", err.Error())
		return err
	}
	return nil
}
