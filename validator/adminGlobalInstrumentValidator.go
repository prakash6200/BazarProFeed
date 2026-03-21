package validator

import (
	"errors"
	"feedprovider/models"
	"strconv"
	"strings"

	"github.com/gofiber/fiber/v2"
)

var allowedGlobalInstrumentStatuses = map[string]struct{}{
	models.GlobalInstrumentStatusActive:   {},
	models.GlobalInstrumentStatusInactive: {},
}

func normalizeGlobalInstrumentStatus(value string) string {
	return strings.ToUpper(strings.TrimSpace(value))
}

func ValidateGlobalInstrument(inst *models.GlobalInstrument) error {
	if strings.TrimSpace(inst.Symbol) == "" {
		return errors.New("symbol is required")
	}

	inst.Symbol = strings.ToUpper(strings.TrimSpace(inst.Symbol))
	inst.Name = strings.TrimSpace(inst.Name)
	inst.Status = normalizeGlobalInstrumentStatus(inst.Status)

	if _, ok := allowedGlobalInstrumentStatuses[inst.Status]; !ok {
		return errors.New("status must be ACTIVE or INACTIVE")
	}
	return nil
}

type GlobalInstrumentListQueryRequest struct {
	Page           int
	Limit          int
	IncludeDeleted bool
	Status         string
	Search         string
}

func ValidateListGlobalInstrumentsQuery(c *fiber.Ctx) error {
	page := 1
	if rawPage := strings.TrimSpace(c.Query("page")); rawPage != "" {
		parsed, err := strconv.Atoi(rawPage)
		if err != nil || parsed < 1 {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"status_code": fiber.StatusBadRequest,
				"message":     "page must be a positive integer",
				"error":       "page must be a positive integer",
			})
		}
		page = parsed
	}

	limit := 20
	if rawLimit := strings.TrimSpace(c.Query("limit")); rawLimit != "" {
		parsed, err := strconv.Atoi(rawLimit)
		if err != nil || parsed < 1 {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"status_code": fiber.StatusBadRequest,
				"message":     "limit must be a positive integer",
				"error":       "limit must be a positive integer",
			})
		}
		if parsed > 100 {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"status_code": fiber.StatusBadRequest,
				"message":     "limit must be less than or equal to 100",
				"error":       "limit must be less than or equal to 100",
			})
		}
		limit = parsed
	}

	includeDeleted := false
	if rawIncludeDeleted := strings.TrimSpace(c.Query("include_deleted")); rawIncludeDeleted != "" {
		parsed, err := strconv.ParseBool(rawIncludeDeleted)
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"status_code": fiber.StatusBadRequest,
				"message":     "include_deleted must be true or false",
				"error":       "include_deleted must be true or false",
			})
		}
		includeDeleted = parsed
	}

	status := normalizeGlobalInstrumentStatus(c.Query("status"))
	search := strings.TrimSpace(c.Query("search"))

	if status != "" {
		if _, ok := allowedGlobalInstrumentStatuses[status]; !ok {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"status_code": fiber.StatusBadRequest,
				"message":     "status must be ACTIVE or INACTIVE",
				"error":       "status must be ACTIVE or INACTIVE",
			})
		}
	}

	if len(search) > 100 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"status_code": fiber.StatusBadRequest,
			"message":     "search must be less than or equal to 100 characters",
			"error":       "search must be less than or equal to 100 characters",
		})
	}

	c.Locals("validated_global_instrument_list_query", GlobalInstrumentListQueryRequest{
		Page:           page,
		Limit:          limit,
		IncludeDeleted: includeDeleted,
		Status:         status,
		Search:         search,
	})

	return c.Next()
}
