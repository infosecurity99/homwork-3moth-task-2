package api

import (
	"github.com/gin-gonic/gin"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
	_ "test/api/docs"
	"test/api/handler"
	"test/storage"
)

// New ...
// @title           Swagger Example API
// @version         1.0
// @description     This is a sample server celler server.
func New(store storage.IStorage) *gin.Engine {
	h := handler.New(store)

	r := gin.New()
//finish user
	r.POST("/user", h.CreateUser)
	r.GET("/user/:id", h.GetUser)
	r.GET("/users", h.GetUserList)
	r.PUT("/user/:id", h.UpdateUser)
	r.DELETE("/user/:id", h.DeleteUser)
	r.PATCH("/user/:id", h.UpdateUserPassword)
//finish category
	r.POST("/category", h.CreateCategory)
	r.GET("/category/:id", h.GetCategory)
	r.GET("/categories", h.GetCategoryList)
	r.PUT("/category/:id", h.UpdateCategory)
	r.DELETE("/category/:id", h.DeleteCategory)
//finish product
	r.POST("/product", h.CreateProduct)
	r.GET("/product/:id", h.GetProduct)
	r.GET("/products", h.GetProductList)
	r.PUT("/product/:id", h.UpdateProduct)
	r.DELETE("/product/:id", h.DeleteProduct)
//finish basket
	r.POST("/basket", h.CreateBasket)
	r.GET("/basket/:id", h.GetBasket)
	r.GET("/baskets", h.GetBasketList)
	r.PUT("basket/:id", h.UpdateBasket)
	r.DELETE("basket/:id", h.DeleteBasket)
//finish basketproduct
	r.POST("/basketProduct", h.CreateBasketProduct)
	r.GET("/basketProduct/:id", h.GetBasketProduct)
	r.GET("/basketProducts", h.GetBasketProductList)
	r.PUT("/basketProduct/:id", h.UpdateBasketProduct)
	r.DELETE("/basketProduct/:id", h.DeleteBasketProduct)

	r.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))
	return r
}
