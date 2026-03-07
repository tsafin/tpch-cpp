/**
 * dsdgen_converter.cpp — Convert dsdgen C structs to Arrow array builders.
 *
 * Uses dectof() to convert decimal_t (scaled integer) fields to double.
 * ds_key_t (= int64_t on Linux) is mapped to arrow::int64().
 */

#include "tpch/dsdgen_converter.hpp"

#include <stdexcept>
#include <arrow/builder.h>

extern "C" {
#include "tpcds_dsdgen.h"
}

namespace tpcds {

// ---------------------------------------------------------------------------
// Helper: decimal_t → double
//
// dsdgen stores decimals as scaled integers: number = value * 10^precision.
// Example: "12.34" → scale=2, precision=2, number=1234.
// Conversion: (double)number / 10^precision.
//
// NOTE: dectoflt() in decimal.c is buggy (divides by 10^(precision-1) and
// mutates the struct).  We implement the correct formula here.
// ---------------------------------------------------------------------------

static inline double dec_to_double(const decimal_t* d) {
    if (d->precision == 0) return static_cast<double>(d->number);
    double result = static_cast<double>(d->number);
    for (int i = 0; i < d->precision; ++i) {
        result /= 10.0;
    }
    return result;
}

// ---------------------------------------------------------------------------
// dict8 encoding helpers — O(1) or O(N) encode for known distributions
// ---------------------------------------------------------------------------
namespace {

static inline int8_t encode_cd_gender(const char* s) { return s[0]=='M'?0:1; }

static inline int8_t encode_cd_marital_status(const char* s) {
    switch(s[0]) { case 'M':return 0; case 'S':return 1; case 'D':return 2;
                   case 'W':return 3; default:return 4; }
}

static inline int8_t encode_cd_education_status(const char* s) {
    switch(s[0]) { case 'P':return 0; case 'S':return 1; case 'C':return 2;
                   case '2':return 3; case '4':return 4; case 'A':return 5; default:return 6; }
}

static inline int8_t encode_cd_credit_rating(const char* s) {
    switch(s[0]) { case 'G':return 0; case 'L':return 1; case 'H':return 2; default:return 3; }
}

static inline int8_t encode_c_salutation(const char* s) {
    if(s[0]=='M') { if(s[1]=='r') return s[2]=='.'?0:1; return s[1]=='s'?2:3; }
    return s[0]=='S'?4:5;
}

static inline int8_t encode_ca_location_type(const char* s) {
    switch(s[0]) { case 's':return 0; case 'c':return 1; default:return 2; }
}

static inline int8_t encode_ca_street_type(const char* s) {
    static const char* types[] = {
        "Street","ST","Avenue","Ave","Boulevard","Blvd","Road","RD",
        "Parkway","Pkwy","Way","Wy","Drive","Dr.","Circle","Cir.","Lane","Ln","Court","Ct."
    };
    for (int i = 0; i < 20; i++) if (strcmp(s, types[i]) == 0) return (int8_t)i;
    return 0;
}

static inline int8_t encode_cc_class(const char* s) {
    switch(s[0]) { case 's':return 0; case 'm':return 1; default:return 2; }
}

static inline int8_t encode_cc_hours(const char* s) {
    return s[5]=='4'?0:(s[5]=='1'?1:2);
}

static inline int8_t encode_cc_name(const char* s) {
    static const char* names[] = {
        "New England","NY Metro","Mid Atlantic","Southeastern","North Midwest",
        "Central Midwest","South Midwest","Pacific Northwest",
        "California","Southwest","Hawaii/Alaska","Other"
    };
    for (int i = 0; i < 12; i++) if (strcmp(s, names[i]) == 0) return (int8_t)i;
    return 0;
}

static inline int8_t encode_cp_type(const char* s) {
    switch(s[0]) { case 'b':return 0; case 'q':return 1; default:return 2; }
}

static inline int8_t encode_wp_type(const char* s) {
    switch(s[0]) { case 'a':return 3; case 'f':return 4; case 'p':return 5; case 'd':return 6;
                   case 'w':return 2; case 'o':return 1; default:return 0; }
}

static inline int8_t encode_sm_type(const char* s) {
    switch(s[0]) { case 'R':return 0; case 'E':return 1; case 'N':return 2;
                   case 'O':return 3; case 'T':return 4; default:return 5; }
}

static inline int8_t encode_sm_code(const char* s) {
    switch(s[0]) { case 'A':return 0; case 'B':return 3; case 'H':return 4;
                   case 'M':return 5; case 'C':return 6;
                   default: return s[1]=='U'?1:2; }
}

static inline int8_t encode_sm_carrier(const char* s) {
    static const char* carriers[] = {
        "UPS","FEDEX","AIRBORNE","USPS","DHL","TBS","ZHOU","ZOUROS","MSC","LATVIAN",
        "ALLIANCE","ORIENTAL","BARIAN","BOXBUNDLES","GREAT EASTERN","DIAMOND",
        "RUPEKSA","GERMA","HARMSTORF","PRIVATECARRIER"
    };
    for (int i = 0; i < 20; i++) if (strcmp(s, carriers[i]) == 0) return (int8_t)i;
    return 0;
}

static inline int8_t encode_t_am_pm(const char* s) { return s[0]=='A'?0:1; }

static inline int8_t encode_t_shift(const char* s) {
    switch(s[0]) { case 'f':return 0; case 's':return 1; default:return 2; }
}

static inline int8_t encode_t_sub_shift(const char* s) {
    switch(s[0]) { case 'm':return 0; case 'a':return 1; case 'e':return 2; default:return 3; }
}

static inline int8_t encode_t_meal_time(const char* s) {
    if(!s || !s[0]) return 0;
    switch(s[0]) { case 'b':return 1; case 'l':return 2; default:return 3; }
}

static inline int8_t encode_d_day_name(const char* s) {
    if(s[0]=='S') return s[1]=='u'?0:6;
    switch(s[0]) { case 'M':return 1; case 'F':return 5;
                   case 'T': return s[1]=='u'?2:4; default:return 3; }
}

static inline int8_t encode_i_category(const char* s) {
    switch(s[0]) {
        case 'W':return 0; case 'C':return 2; case 'J':return 5;
        case 'H':return 6; case 'B':return 8; case 'E':return 9;
        case 'S': return s[1]=='h'?3:7;
        case 'M': return s[1]=='e'?1:4;
        default:return 0;
    }
}

static inline int8_t encode_i_size(const char* s) {
    switch(s[0]) { case 'p':return 0; case 's':return 1; case 'm':return 2;
                   case 'l':return 3; case 'e':return 4;
                   case 'N':return 6; default:return 5; }
}

static inline int8_t encode_i_color(const char* s) {
    static const char* colors[] = {
        "almond","antique","aquamarine","azure","beige","bisque","black","blanched",
        "blue","blush","brown","burlywood","burnished","chartreuse","chiffon","chocolate",
        "coral","cornflower","cornsilk","cream","cyan","dark","deep","dim","dodger",
        "drab","firebrick","floral","forest","frosted","gainsboro","ghost","goldenrod",
        "green","grey","honeydew","hot","indian","ivory","khaki","lace","lavender",
        "lawn","lemon","light","lime","linen","magenta","maroon","medium","metallic",
        "midnight","mint","misty","moccasin","navajo","navy","olive","orange","orchid",
        "pale","papaya","peach","peru","pink","plum","powder","puff","purple","red",
        "rose","rosy","royal","saddle","salmon","sandy","seashell","sienna","sky",
        "slate","smoke","snow","spring","steel","tan","thistle","tomato","turquoise",
        "violet","wheat","white","yellow"
    };
    for (int i = 0; i < 92; i++) if (strcmp(s, colors[i]) == 0) return (int8_t)i;
    return 0;
}

static inline int8_t encode_i_units(const char* s) {
    static const char* units[] = {
        "Unknown","Each","Dozen","Case","Pallet","Gross","Carton","Box","Bunch",
        "Bundle","Oz","Lb","Ton","Ounce","Pound","Tsp","Tbl","Cup","Dram","Gram","N/A"
    };
    for (int i = 0; i < 21; i++) if (strcmp(s, units[i]) == 0) return (int8_t)i;
    return 0;
}

static inline int8_t encode_state(const char* s) {
    static const char* states[] = {
        "AK","AL","AR","AZ","CA","CO","CT","DC","DE","FL","GA","HI","IA","ID",
        "IL","IN","KS","KY","LA","MA","MD","ME","MI","MN","MO","MS","MT","NC",
        "ND","NE","NH","NJ","NM","NV","NY","OH","OK","OR","PA","RI","SC","SD",
        "TN","TX","UT","VA","VT","WA","WI","WV","WY"
    };
    for (int i = 0; i < 52; i++) if (strcmp(s, states[i]) == 0) return (int8_t)i;
    return 0;
}

}  // anonymous namespace

// ---------------------------------------------------------------------------
// Static dictionary arrays and getter
// ---------------------------------------------------------------------------

std::shared_ptr<arrow::Array> get_dict_for_field(const std::string& name) {
    auto make = [](std::initializer_list<const char*> vals) {
        arrow::StringBuilder b;
        for (auto v : vals) (void)b.Append(v, strlen(v));
        return *b.Finish();
    };

    static auto gender       = make({"M","F"});
    static auto marital      = make({"M","S","D","W","U"});
    static auto education    = make({"Primary","Secondary","College","2 yr Degree","4 yr Degree","Advanced Degree","Unknown"});
    static auto credit       = make({"Good","Low Risk","High Risk","Unknown"});
    static auto salutation   = make({"Mr.","Mrs.","Ms.","Miss","Sir","Dr."});
    static auto am_pm        = make({"AM","PM"});
    static auto shift        = make({"first","second","third"});
    static auto sub_shift    = make({"morning","afternoon","evening","night"});
    static auto meal_time    = make({"","breakfast","lunch","dinner"});
    static auto day_name     = make({"Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"});
    static auto category     = make({"Women","Men","Children","Shoes","Music","Jewelry","Home","Sports","Books","Electronics"});
    static auto item_size    = make({"petite","small","medium","large","extra large","economy","N/A"});
    static auto cp_type_d    = make({"bi-annual","quarterly","monthly"});
    static auto wp_type_d    = make({"general","order","welcome","ad","feedback","protected","dynamic"});
    static auto sm_type_d    = make({"REGULAR","EXPRESS","NEXT DAY","OVERNIGHT","TWO DAY","LIBRARY"});
    static auto sm_code_d    = make({"AIR","SURFACE","SEA","BIKE","HAND CARRY","MESSENGER","COURIER"});
    static auto sm_carrier_d = make({"UPS","FEDEX","AIRBORNE","USPS","DHL","TBS","ZHOU","ZOUROS","MSC","LATVIAN","ALLIANCE","ORIENTAL","BARIAN","BOXBUNDLES","GREAT EASTERN","DIAMOND","RUPEKSA","GERMA","HARMSTORF","PRIVATECARRIER"});
    static auto loc_type     = make({"single family","condo","apartment"});
    static auto cc_class_d   = make({"small","medium","large"});
    static auto cc_hours_d   = make({"8AM-4PM","8AM-12AM","8AM-8AM"});
    static auto cc_name_d    = make({"New England","NY Metro","Mid Atlantic","Southeastern","North Midwest","Central Midwest","South Midwest","Pacific Northwest","California","Southwest","Hawaii/Alaska","Other"});
    static auto street_type_d = make({"Street","ST","Avenue","Ave","Boulevard","Blvd","Road","RD","Parkway","Pkwy","Way","Wy","Drive","Dr.","Circle","Cir.","Lane","Ln","Court","Ct."});
    static auto one_unknown  = make({"Unknown"});
    static auto one_dept     = make({"DEPARTMENT"});
    static auto one_us       = make({"United States"});
    static auto states       = make({"AK","AL","AR","AZ","CA","CO","CT","DC","DE","FL","GA","HI","IA","ID","IL","IN","KS","KY","LA","MA","MD","ME","MI","MN","MO","MS","MT","NC","ND","NE","NH","NJ","NM","NV","NY","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VA","VT","WA","WI","WV","WY"});
    static auto colors       = make({"almond","antique","aquamarine","azure","beige","bisque","black","blanched","blue","blush","brown","burlywood","burnished","chartreuse","chiffon","chocolate","coral","cornflower","cornsilk","cream","cyan","dark","deep","dim","dodger","drab","firebrick","floral","forest","frosted","gainsboro","ghost","goldenrod","green","grey","honeydew","hot","indian","ivory","khaki","lace","lavender","lawn","lemon","light","lime","linen","magenta","maroon","medium","metallic","midnight","mint","misty","moccasin","navajo","navy","olive","orange","orchid","pale","papaya","peach","peru","pink","plum","powder","puff","purple","red","rose","rosy","royal","saddle","salmon","sandy","seashell","sienna","sky","slate","smoke","snow","spring","steel","tan","thistle","tomato","turquoise","violet","wheat","white","yellow"});
    static auto units        = make({"Unknown","Each","Dozen","Case","Pallet","Gross","Carton","Box","Bunch","Bundle","Oz","Lb","Ton","Ounce","Pound","Tsp","Tbl","Cup","Dram","Gram","N/A"});

    static const std::unordered_map<std::string, std::shared_ptr<arrow::Array>> registry = {
        {"cd_gender", gender},
        {"cd_marital_status", marital},
        {"cd_education_status", education},
        {"cd_credit_rating", credit},
        {"c_salutation", salutation},
        {"t_am_pm", am_pm},
        {"t_shift", shift},
        {"t_sub_shift", sub_shift},
        {"t_meal_time", meal_time},
        {"d_day_name", day_name},
        {"i_category", category},
        {"i_size", item_size},
        {"i_container", one_unknown},
        {"i_color", colors},
        {"i_units", units},
        {"cp_department", one_dept},
        {"cp_type", cp_type_d},
        {"wp_type", wp_type_d},
        {"web_class", one_unknown},
        {"web_country", one_us},
        {"web_state", states},
        {"web_street_type", street_type_d},
        {"w_country", one_us},
        {"w_state", states},
        {"w_street_type", street_type_d},
        {"s_hours", cc_hours_d},
        {"s_geography_class", one_unknown},
        {"s_division_name", one_unknown},
        {"s_company_name", one_unknown},
        {"s_country", one_us},
        {"s_state", states},
        {"s_street_type", street_type_d},
        {"sm_type", sm_type_d},
        {"sm_code", sm_code_d},
        {"sm_carrier", sm_carrier_d},
        {"cc_class", cc_class_d},
        {"cc_hours", cc_hours_d},
        {"cc_name", cc_name_d},
        {"cc_country", one_us},
        {"cc_state", states},
        {"cc_street_type", street_type_d},
        {"ca_location_type", loc_type},
        {"ca_country", one_us},
        {"ca_state", states},
        {"ca_street_type", street_type_d},
        {"p_purpose", one_unknown},
    };

    auto it = registry.find(name);
    return it != registry.end() ? it->second : nullptr;
}

// ---------------------------------------------------------------------------
// store_sales
// ---------------------------------------------------------------------------

void append_store_sales_to_builders(
    const void* row,
    tpcds::BuilderMap& builders)
{
    auto* r = static_cast<const W_STORE_SALES_TBL*>(row);

    // Surrogate keys (int64)
    static_cast<arrow::Int64Builder*>(builders["ss_sold_date_sk"].get())
        ->Append(static_cast<int64_t>(r->ss_sold_date_sk));
    static_cast<arrow::Int64Builder*>(builders["ss_sold_time_sk"].get())
        ->Append(static_cast<int64_t>(r->ss_sold_time_sk));
    static_cast<arrow::Int64Builder*>(builders["ss_item_sk"].get())
        ->Append(static_cast<int64_t>(r->ss_sold_item_sk));
    static_cast<arrow::Int64Builder*>(builders["ss_customer_sk"].get())
        ->Append(static_cast<int64_t>(r->ss_sold_customer_sk));
    static_cast<arrow::Int64Builder*>(builders["ss_cdemo_sk"].get())
        ->Append(static_cast<int64_t>(r->ss_sold_cdemo_sk));
    static_cast<arrow::Int64Builder*>(builders["ss_hdemo_sk"].get())
        ->Append(static_cast<int64_t>(r->ss_sold_hdemo_sk));
    static_cast<arrow::Int64Builder*>(builders["ss_addr_sk"].get())
        ->Append(static_cast<int64_t>(r->ss_sold_addr_sk));
    static_cast<arrow::Int64Builder*>(builders["ss_store_sk"].get())
        ->Append(static_cast<int64_t>(r->ss_sold_store_sk));
    static_cast<arrow::Int64Builder*>(builders["ss_promo_sk"].get())
        ->Append(static_cast<int64_t>(r->ss_sold_promo_sk));
    static_cast<arrow::Int64Builder*>(builders["ss_ticket_number"].get())
        ->Append(static_cast<int64_t>(r->ss_ticket_number));

    // Quantity (int)
    static_cast<arrow::Int32Builder*>(builders["ss_quantity"].get())
        ->Append(static_cast<int32_t>(r->ss_pricing.quantity));

    // Decimal pricing fields → double
    const ds_pricing_t* p = &r->ss_pricing;

    static_cast<arrow::DoubleBuilder*>(builders["ss_wholesale_cost"].get())
        ->Append(dec_to_double(&p->wholesale_cost));
    static_cast<arrow::DoubleBuilder*>(builders["ss_list_price"].get())
        ->Append(dec_to_double(&p->list_price));
    static_cast<arrow::DoubleBuilder*>(builders["ss_sales_price"].get())
        ->Append(dec_to_double(&p->sales_price));
    static_cast<arrow::DoubleBuilder*>(builders["ss_ext_discount_amt"].get())
        ->Append(dec_to_double(&p->ext_discount_amt));
    static_cast<arrow::DoubleBuilder*>(builders["ss_ext_sales_price"].get())
        ->Append(dec_to_double(&p->ext_sales_price));
    static_cast<arrow::DoubleBuilder*>(builders["ss_ext_wholesale_cost"].get())
        ->Append(dec_to_double(&p->ext_wholesale_cost));
    static_cast<arrow::DoubleBuilder*>(builders["ss_ext_list_price"].get())
        ->Append(dec_to_double(&p->ext_list_price));
    static_cast<arrow::DoubleBuilder*>(builders["ss_ext_tax"].get())
        ->Append(dec_to_double(&p->ext_tax));
    static_cast<arrow::DoubleBuilder*>(builders["ss_coupon_amt"].get())
        ->Append(dec_to_double(&p->coupon_amt));
    static_cast<arrow::DoubleBuilder*>(builders["ss_net_paid"].get())
        ->Append(dec_to_double(&p->net_paid));
    static_cast<arrow::DoubleBuilder*>(builders["ss_net_paid_inc_tax"].get())
        ->Append(dec_to_double(&p->net_paid_inc_tax));
    static_cast<arrow::DoubleBuilder*>(builders["ss_net_profit"].get())
        ->Append(dec_to_double(&p->net_profit));
}

// ---------------------------------------------------------------------------
// inventory
// ---------------------------------------------------------------------------

void append_inventory_to_builders(
    const void* row,
    tpcds::BuilderMap& builders)
{
    auto* r = static_cast<const W_INVENTORY_TBL*>(row);

    static_cast<arrow::Int64Builder*>(builders["inv_date_sk"].get())
        ->Append(static_cast<int64_t>(r->inv_date_sk));
    static_cast<arrow::Int64Builder*>(builders["inv_item_sk"].get())
        ->Append(static_cast<int64_t>(r->inv_item_sk));
    static_cast<arrow::Int64Builder*>(builders["inv_warehouse_sk"].get())
        ->Append(static_cast<int64_t>(r->inv_warehouse_sk));
    static_cast<arrow::Int32Builder*>(builders["inv_quantity_on_hand"].get())
        ->Append(static_cast<int32_t>(r->inv_quantity_on_hand));
}

// ---------------------------------------------------------------------------
// catalog_sales
// ---------------------------------------------------------------------------

void append_catalog_sales_to_builders(
    const void* row,
    tpcds::BuilderMap& builders)
{
    auto* r = static_cast<const W_CATALOG_SALES_TBL*>(row);

    static_cast<arrow::Int64Builder*>(builders["cs_sold_date_sk"].get())
        ->Append(static_cast<int64_t>(r->cs_sold_date_sk));
    static_cast<arrow::Int64Builder*>(builders["cs_sold_time_sk"].get())
        ->Append(static_cast<int64_t>(r->cs_sold_time_sk));
    static_cast<arrow::Int64Builder*>(builders["cs_ship_date_sk"].get())
        ->Append(static_cast<int64_t>(r->cs_ship_date_sk));
    static_cast<arrow::Int64Builder*>(builders["cs_bill_customer_sk"].get())
        ->Append(static_cast<int64_t>(r->cs_bill_customer_sk));
    static_cast<arrow::Int64Builder*>(builders["cs_bill_cdemo_sk"].get())
        ->Append(static_cast<int64_t>(r->cs_bill_cdemo_sk));
    static_cast<arrow::Int64Builder*>(builders["cs_bill_hdemo_sk"].get())
        ->Append(static_cast<int64_t>(r->cs_bill_hdemo_sk));
    static_cast<arrow::Int64Builder*>(builders["cs_bill_addr_sk"].get())
        ->Append(static_cast<int64_t>(r->cs_bill_addr_sk));
    static_cast<arrow::Int64Builder*>(builders["cs_ship_customer_sk"].get())
        ->Append(static_cast<int64_t>(r->cs_ship_customer_sk));
    static_cast<arrow::Int64Builder*>(builders["cs_ship_cdemo_sk"].get())
        ->Append(static_cast<int64_t>(r->cs_ship_cdemo_sk));
    static_cast<arrow::Int64Builder*>(builders["cs_ship_hdemo_sk"].get())
        ->Append(static_cast<int64_t>(r->cs_ship_hdemo_sk));
    static_cast<arrow::Int64Builder*>(builders["cs_ship_addr_sk"].get())
        ->Append(static_cast<int64_t>(r->cs_ship_addr_sk));
    static_cast<arrow::Int64Builder*>(builders["cs_call_center_sk"].get())
        ->Append(static_cast<int64_t>(r->cs_call_center_sk));
    static_cast<arrow::Int64Builder*>(builders["cs_catalog_page_sk"].get())
        ->Append(static_cast<int64_t>(r->cs_catalog_page_sk));
    static_cast<arrow::Int64Builder*>(builders["cs_ship_mode_sk"].get())
        ->Append(static_cast<int64_t>(r->cs_ship_mode_sk));
    static_cast<arrow::Int64Builder*>(builders["cs_warehouse_sk"].get())
        ->Append(static_cast<int64_t>(r->cs_warehouse_sk));
    static_cast<arrow::Int64Builder*>(builders["cs_item_sk"].get())
        ->Append(static_cast<int64_t>(r->cs_sold_item_sk));
    static_cast<arrow::Int64Builder*>(builders["cs_promo_sk"].get())
        ->Append(static_cast<int64_t>(r->cs_promo_sk));
    static_cast<arrow::Int64Builder*>(builders["cs_order_number"].get())
        ->Append(static_cast<int64_t>(r->cs_order_number));

    const ds_pricing_t* p = &r->cs_pricing;
    static_cast<arrow::Int32Builder*>(builders["cs_quantity"].get())
        ->Append(static_cast<int32_t>(p->quantity));
    static_cast<arrow::DoubleBuilder*>(builders["cs_wholesale_cost"].get())
        ->Append(dec_to_double(&p->wholesale_cost));
    static_cast<arrow::DoubleBuilder*>(builders["cs_list_price"].get())
        ->Append(dec_to_double(&p->list_price));
    static_cast<arrow::DoubleBuilder*>(builders["cs_sales_price"].get())
        ->Append(dec_to_double(&p->sales_price));
    static_cast<arrow::DoubleBuilder*>(builders["cs_ext_discount_amt"].get())
        ->Append(dec_to_double(&p->ext_discount_amt));
    static_cast<arrow::DoubleBuilder*>(builders["cs_ext_sales_price"].get())
        ->Append(dec_to_double(&p->ext_sales_price));
    static_cast<arrow::DoubleBuilder*>(builders["cs_ext_wholesale_cost"].get())
        ->Append(dec_to_double(&p->ext_wholesale_cost));
    static_cast<arrow::DoubleBuilder*>(builders["cs_ext_list_price"].get())
        ->Append(dec_to_double(&p->ext_list_price));
    static_cast<arrow::DoubleBuilder*>(builders["cs_ext_tax"].get())
        ->Append(dec_to_double(&p->ext_tax));
    static_cast<arrow::DoubleBuilder*>(builders["cs_coupon_amt"].get())
        ->Append(dec_to_double(&p->coupon_amt));
    static_cast<arrow::DoubleBuilder*>(builders["cs_ext_ship_cost"].get())
        ->Append(dec_to_double(&p->ext_ship_cost));
    static_cast<arrow::DoubleBuilder*>(builders["cs_net_paid"].get())
        ->Append(dec_to_double(&p->net_paid));
    static_cast<arrow::DoubleBuilder*>(builders["cs_net_paid_inc_tax"].get())
        ->Append(dec_to_double(&p->net_paid_inc_tax));
    static_cast<arrow::DoubleBuilder*>(builders["cs_net_paid_inc_ship"].get())
        ->Append(dec_to_double(&p->net_paid_inc_ship));
    static_cast<arrow::DoubleBuilder*>(builders["cs_net_paid_inc_ship_tax"].get())
        ->Append(dec_to_double(&p->net_paid_inc_ship_tax));
    static_cast<arrow::DoubleBuilder*>(builders["cs_net_profit"].get())
        ->Append(dec_to_double(&p->net_profit));
}

// ---------------------------------------------------------------------------
// web_sales
// ---------------------------------------------------------------------------

void append_web_sales_to_builders(
    const void* row,
    tpcds::BuilderMap& builders)
{
    auto* r = static_cast<const W_WEB_SALES_TBL*>(row);

    static_cast<arrow::Int64Builder*>(builders["ws_sold_date_sk"].get())
        ->Append(static_cast<int64_t>(r->ws_sold_date_sk));
    static_cast<arrow::Int64Builder*>(builders["ws_sold_time_sk"].get())
        ->Append(static_cast<int64_t>(r->ws_sold_time_sk));
    static_cast<arrow::Int64Builder*>(builders["ws_ship_date_sk"].get())
        ->Append(static_cast<int64_t>(r->ws_ship_date_sk));
    static_cast<arrow::Int64Builder*>(builders["ws_item_sk"].get())
        ->Append(static_cast<int64_t>(r->ws_item_sk));
    static_cast<arrow::Int64Builder*>(builders["ws_bill_customer_sk"].get())
        ->Append(static_cast<int64_t>(r->ws_bill_customer_sk));
    static_cast<arrow::Int64Builder*>(builders["ws_bill_cdemo_sk"].get())
        ->Append(static_cast<int64_t>(r->ws_bill_cdemo_sk));
    static_cast<arrow::Int64Builder*>(builders["ws_bill_hdemo_sk"].get())
        ->Append(static_cast<int64_t>(r->ws_bill_hdemo_sk));
    static_cast<arrow::Int64Builder*>(builders["ws_bill_addr_sk"].get())
        ->Append(static_cast<int64_t>(r->ws_bill_addr_sk));
    static_cast<arrow::Int64Builder*>(builders["ws_ship_customer_sk"].get())
        ->Append(static_cast<int64_t>(r->ws_ship_customer_sk));
    static_cast<arrow::Int64Builder*>(builders["ws_ship_cdemo_sk"].get())
        ->Append(static_cast<int64_t>(r->ws_ship_cdemo_sk));
    static_cast<arrow::Int64Builder*>(builders["ws_ship_hdemo_sk"].get())
        ->Append(static_cast<int64_t>(r->ws_ship_hdemo_sk));
    static_cast<arrow::Int64Builder*>(builders["ws_ship_addr_sk"].get())
        ->Append(static_cast<int64_t>(r->ws_ship_addr_sk));
    static_cast<arrow::Int64Builder*>(builders["ws_web_page_sk"].get())
        ->Append(static_cast<int64_t>(r->ws_web_page_sk));
    static_cast<arrow::Int64Builder*>(builders["ws_web_site_sk"].get())
        ->Append(static_cast<int64_t>(r->ws_web_site_sk));
    static_cast<arrow::Int64Builder*>(builders["ws_ship_mode_sk"].get())
        ->Append(static_cast<int64_t>(r->ws_ship_mode_sk));
    static_cast<arrow::Int64Builder*>(builders["ws_warehouse_sk"].get())
        ->Append(static_cast<int64_t>(r->ws_warehouse_sk));
    static_cast<arrow::Int64Builder*>(builders["ws_promo_sk"].get())
        ->Append(static_cast<int64_t>(r->ws_promo_sk));
    static_cast<arrow::Int64Builder*>(builders["ws_order_number"].get())
        ->Append(static_cast<int64_t>(r->ws_order_number));

    const ds_pricing_t* p = &r->ws_pricing;
    static_cast<arrow::Int32Builder*>(builders["ws_quantity"].get())
        ->Append(static_cast<int32_t>(p->quantity));
    static_cast<arrow::DoubleBuilder*>(builders["ws_wholesale_cost"].get())
        ->Append(dec_to_double(&p->wholesale_cost));
    static_cast<arrow::DoubleBuilder*>(builders["ws_list_price"].get())
        ->Append(dec_to_double(&p->list_price));
    static_cast<arrow::DoubleBuilder*>(builders["ws_sales_price"].get())
        ->Append(dec_to_double(&p->sales_price));
    static_cast<arrow::DoubleBuilder*>(builders["ws_ext_discount_amt"].get())
        ->Append(dec_to_double(&p->ext_discount_amt));
    static_cast<arrow::DoubleBuilder*>(builders["ws_ext_sales_price"].get())
        ->Append(dec_to_double(&p->ext_sales_price));
    static_cast<arrow::DoubleBuilder*>(builders["ws_ext_wholesale_cost"].get())
        ->Append(dec_to_double(&p->ext_wholesale_cost));
    static_cast<arrow::DoubleBuilder*>(builders["ws_ext_list_price"].get())
        ->Append(dec_to_double(&p->ext_list_price));
    static_cast<arrow::DoubleBuilder*>(builders["ws_ext_tax"].get())
        ->Append(dec_to_double(&p->ext_tax));
    static_cast<arrow::DoubleBuilder*>(builders["ws_coupon_amt"].get())
        ->Append(dec_to_double(&p->coupon_amt));
    static_cast<arrow::DoubleBuilder*>(builders["ws_ext_ship_cost"].get())
        ->Append(dec_to_double(&p->ext_ship_cost));
    static_cast<arrow::DoubleBuilder*>(builders["ws_net_paid"].get())
        ->Append(dec_to_double(&p->net_paid));
    static_cast<arrow::DoubleBuilder*>(builders["ws_net_paid_inc_tax"].get())
        ->Append(dec_to_double(&p->net_paid_inc_tax));
    static_cast<arrow::DoubleBuilder*>(builders["ws_net_paid_inc_ship"].get())
        ->Append(dec_to_double(&p->net_paid_inc_ship));
    static_cast<arrow::DoubleBuilder*>(builders["ws_net_paid_inc_ship_tax"].get())
        ->Append(dec_to_double(&p->net_paid_inc_ship_tax));
    static_cast<arrow::DoubleBuilder*>(builders["ws_net_profit"].get())
        ->Append(dec_to_double(&p->net_profit));
}

// ---------------------------------------------------------------------------
// customer
// ---------------------------------------------------------------------------

void append_customer_to_builders(
    const void* row,
    tpcds::BuilderMap& builders)
{
    auto* r = static_cast<const W_CUSTOMER_TBL*>(row);

    static_cast<arrow::Int64Builder*>(builders["c_customer_sk"].get())
        ->Append(static_cast<int64_t>(r->c_customer_sk));
    static_cast<arrow::StringBuilder*>(builders["c_customer_id"].get())
        ->Append(r->c_customer_id);
    static_cast<arrow::Int64Builder*>(builders["c_current_cdemo_sk"].get())
        ->Append(static_cast<int64_t>(r->c_current_cdemo_sk));
    static_cast<arrow::Int64Builder*>(builders["c_current_hdemo_sk"].get())
        ->Append(static_cast<int64_t>(r->c_current_hdemo_sk));
    static_cast<arrow::Int64Builder*>(builders["c_current_addr_sk"].get())
        ->Append(static_cast<int64_t>(r->c_current_addr_sk));
    static_cast<arrow::Int32Builder*>(builders["c_first_shipto_date_id"].get())
        ->Append(static_cast<int32_t>(r->c_first_shipto_date_id));
    static_cast<arrow::Int32Builder*>(builders["c_first_sales_date_id"].get())
        ->Append(static_cast<int32_t>(r->c_first_sales_date_id));
    static_cast<arrow::Int8Builder*>(builders["c_salutation"].get())
        ->Append(encode_c_salutation(r->c_salutation ? r->c_salutation : ""));
    static_cast<arrow::StringBuilder*>(builders["c_first_name"].get())
        ->Append(r->c_first_name ? r->c_first_name : "");
    static_cast<arrow::StringBuilder*>(builders["c_last_name"].get())
        ->Append(r->c_last_name ? r->c_last_name : "");
    static_cast<arrow::Int32Builder*>(builders["c_preferred_cust_flag"].get())
        ->Append(static_cast<int32_t>(r->c_preferred_cust_flag));
    static_cast<arrow::Int32Builder*>(builders["c_birth_day"].get())
        ->Append(static_cast<int32_t>(r->c_birth_day));
    static_cast<arrow::Int32Builder*>(builders["c_birth_month"].get())
        ->Append(static_cast<int32_t>(r->c_birth_month));
    static_cast<arrow::Int32Builder*>(builders["c_birth_year"].get())
        ->Append(static_cast<int32_t>(r->c_birth_year));
    static_cast<arrow::StringBuilder*>(builders["c_birth_country"].get())
        ->Append(r->c_birth_country ? r->c_birth_country : "");
    static_cast<arrow::StringBuilder*>(builders["c_login"].get())
        ->Append(r->c_login);
    static_cast<arrow::StringBuilder*>(builders["c_email_address"].get())
        ->Append(r->c_email_address);
    static_cast<arrow::Int32Builder*>(builders["c_last_review_date"].get())
        ->Append(static_cast<int32_t>(r->c_last_review_date));
}

// ---------------------------------------------------------------------------
// item
// ---------------------------------------------------------------------------

void append_item_to_builders(
    const void* row,
    tpcds::BuilderMap& builders)
{
    auto* r = static_cast<const W_ITEM_TBL*>(row);

    static_cast<arrow::Int64Builder*>(builders["i_item_sk"].get())
        ->Append(static_cast<int64_t>(r->i_item_sk));
    static_cast<arrow::StringBuilder*>(builders["i_item_id"].get())
        ->Append(r->i_item_id);
    static_cast<arrow::Int64Builder*>(builders["i_rec_start_date_id"].get())
        ->Append(static_cast<int64_t>(r->i_rec_start_date_id));
    static_cast<arrow::Int64Builder*>(builders["i_rec_end_date_id"].get())
        ->Append(static_cast<int64_t>(r->i_rec_end_date_id));
    static_cast<arrow::StringBuilder*>(builders["i_item_desc"].get())
        ->Append(r->i_item_desc);
    static_cast<arrow::DoubleBuilder*>(builders["i_current_price"].get())
        ->Append(dec_to_double(&r->i_current_price));
    static_cast<arrow::DoubleBuilder*>(builders["i_wholesale_cost"].get())
        ->Append(dec_to_double(&r->i_wholesale_cost));
    static_cast<arrow::Int64Builder*>(builders["i_brand_id"].get())
        ->Append(static_cast<int64_t>(r->i_brand_id));
    static_cast<arrow::StringBuilder*>(builders["i_brand"].get())
        ->Append(r->i_brand);
    static_cast<arrow::Int64Builder*>(builders["i_class_id"].get())
        ->Append(static_cast<int64_t>(r->i_class_id));
    static_cast<arrow::StringBuilder*>(builders["i_class"].get())
        ->Append(r->i_class ? r->i_class : "");
    static_cast<arrow::Int64Builder*>(builders["i_category_id"].get())
        ->Append(static_cast<int64_t>(r->i_category_id));
    static_cast<arrow::Int8Builder*>(builders["i_category"].get())
        ->Append(encode_i_category(r->i_category ? r->i_category : ""));
    static_cast<arrow::Int64Builder*>(builders["i_manufact_id"].get())
        ->Append(static_cast<int64_t>(r->i_manufact_id));
    static_cast<arrow::StringBuilder*>(builders["i_manufact"].get())
        ->Append(r->i_manufact);
    static_cast<arrow::Int8Builder*>(builders["i_size"].get())
        ->Append(encode_i_size(r->i_size ? r->i_size : ""));
    static_cast<arrow::StringBuilder*>(builders["i_formulation"].get())
        ->Append(r->i_formulation);
    static_cast<arrow::Int8Builder*>(builders["i_color"].get())
        ->Append(encode_i_color(r->i_color ? r->i_color : ""));
    static_cast<arrow::Int8Builder*>(builders["i_units"].get())
        ->Append(encode_i_units(r->i_units ? r->i_units : ""));
    static_cast<arrow::Int8Builder*>(builders["i_container"].get())
        ->Append(0);  // always "Unknown"
    static_cast<arrow::Int64Builder*>(builders["i_manager_id"].get())
        ->Append(static_cast<int64_t>(r->i_manager_id));
    static_cast<arrow::StringBuilder*>(builders["i_product_name"].get())
        ->Append(r->i_product_name);
    static_cast<arrow::Int64Builder*>(builders["i_promo_sk"].get())
        ->Append(static_cast<int64_t>(r->i_promo_sk));
}

// ---------------------------------------------------------------------------
// date_dim
// ---------------------------------------------------------------------------

void append_date_dim_to_builders(
    const void* row,
    tpcds::BuilderMap& builders)
{
    auto* r = static_cast<const W_DATE_TBL*>(row);

    static_cast<arrow::Int64Builder*>(builders["d_date_sk"].get())
        ->Append(static_cast<int64_t>(r->d_date_sk));
    static_cast<arrow::StringBuilder*>(builders["d_date_id"].get())
        ->Append(r->d_date_id);
    static_cast<arrow::Int32Builder*>(builders["d_month_seq"].get())
        ->Append(static_cast<int32_t>(r->d_month_seq));
    static_cast<arrow::Int32Builder*>(builders["d_week_seq"].get())
        ->Append(static_cast<int32_t>(r->d_week_seq));
    static_cast<arrow::Int32Builder*>(builders["d_quarter_seq"].get())
        ->Append(static_cast<int32_t>(r->d_quarter_seq));
    static_cast<arrow::Int32Builder*>(builders["d_year"].get())
        ->Append(static_cast<int32_t>(r->d_year));
    static_cast<arrow::Int32Builder*>(builders["d_dow"].get())
        ->Append(static_cast<int32_t>(r->d_dow));
    static_cast<arrow::Int32Builder*>(builders["d_moy"].get())
        ->Append(static_cast<int32_t>(r->d_moy));
    static_cast<arrow::Int32Builder*>(builders["d_dom"].get())
        ->Append(static_cast<int32_t>(r->d_dom));
    static_cast<arrow::Int32Builder*>(builders["d_qoy"].get())
        ->Append(static_cast<int32_t>(r->d_qoy));
    static_cast<arrow::Int32Builder*>(builders["d_fy_year"].get())
        ->Append(static_cast<int32_t>(r->d_fy_year));
    static_cast<arrow::Int32Builder*>(builders["d_fy_quarter_seq"].get())
        ->Append(static_cast<int32_t>(r->d_fy_quarter_seq));
    static_cast<arrow::Int32Builder*>(builders["d_fy_week_seq"].get())
        ->Append(static_cast<int32_t>(r->d_fy_week_seq));
    static_cast<arrow::Int8Builder*>(builders["d_day_name"].get())
        ->Append(encode_d_day_name(r->d_day_name ? r->d_day_name : ""));
    static_cast<arrow::Int32Builder*>(builders["d_holiday"].get())
        ->Append(static_cast<int32_t>(r->d_holiday));
    static_cast<arrow::Int32Builder*>(builders["d_weekend"].get())
        ->Append(static_cast<int32_t>(r->d_weekend));
    static_cast<arrow::Int32Builder*>(builders["d_following_holiday"].get())
        ->Append(static_cast<int32_t>(r->d_following_holiday));
    static_cast<arrow::Int32Builder*>(builders["d_first_dom"].get())
        ->Append(static_cast<int32_t>(r->d_first_dom));
    static_cast<arrow::Int32Builder*>(builders["d_last_dom"].get())
        ->Append(static_cast<int32_t>(r->d_last_dom));
    static_cast<arrow::Int32Builder*>(builders["d_same_day_ly"].get())
        ->Append(static_cast<int32_t>(r->d_same_day_ly));
    static_cast<arrow::Int32Builder*>(builders["d_same_day_lq"].get())
        ->Append(static_cast<int32_t>(r->d_same_day_lq));
    static_cast<arrow::Int32Builder*>(builders["d_current_day"].get())
        ->Append(static_cast<int32_t>(r->d_current_day));
    static_cast<arrow::Int32Builder*>(builders["d_current_week"].get())
        ->Append(static_cast<int32_t>(r->d_current_week));
    static_cast<arrow::Int32Builder*>(builders["d_current_month"].get())
        ->Append(static_cast<int32_t>(r->d_current_month));
    static_cast<arrow::Int32Builder*>(builders["d_current_quarter"].get())
        ->Append(static_cast<int32_t>(r->d_current_quarter));
    static_cast<arrow::Int32Builder*>(builders["d_current_year"].get())
        ->Append(static_cast<int32_t>(r->d_current_year));
}

// ---------------------------------------------------------------------------
// store_returns
// ---------------------------------------------------------------------------

void append_store_returns_to_builders(
    const void* row,
    tpcds::BuilderMap& builders)
{
    auto* r = static_cast<const W_STORE_RETURNS_TBL*>(row);

    static_cast<arrow::Int64Builder*>(builders["sr_returned_date_sk"].get())
        ->Append(static_cast<int64_t>(r->sr_returned_date_sk));
    static_cast<arrow::Int64Builder*>(builders["sr_returned_time_sk"].get())
        ->Append(static_cast<int64_t>(r->sr_returned_time_sk));
    static_cast<arrow::Int64Builder*>(builders["sr_item_sk"].get())
        ->Append(static_cast<int64_t>(r->sr_item_sk));
    static_cast<arrow::Int64Builder*>(builders["sr_customer_sk"].get())
        ->Append(static_cast<int64_t>(r->sr_customer_sk));
    static_cast<arrow::Int64Builder*>(builders["sr_cdemo_sk"].get())
        ->Append(static_cast<int64_t>(r->sr_cdemo_sk));
    static_cast<arrow::Int64Builder*>(builders["sr_hdemo_sk"].get())
        ->Append(static_cast<int64_t>(r->sr_hdemo_sk));
    static_cast<arrow::Int64Builder*>(builders["sr_addr_sk"].get())
        ->Append(static_cast<int64_t>(r->sr_addr_sk));
    static_cast<arrow::Int64Builder*>(builders["sr_store_sk"].get())
        ->Append(static_cast<int64_t>(r->sr_store_sk));
    static_cast<arrow::Int64Builder*>(builders["sr_reason_sk"].get())
        ->Append(static_cast<int64_t>(r->sr_reason_sk));
    static_cast<arrow::Int64Builder*>(builders["sr_ticket_number"].get())
        ->Append(static_cast<int64_t>(r->sr_ticket_number));

    const ds_pricing_t* p = &r->sr_pricing;
    static_cast<arrow::Int32Builder*>(builders["sr_quantity"].get())
        ->Append(static_cast<int32_t>(p->quantity));
    static_cast<arrow::DoubleBuilder*>(builders["sr_net_paid"].get())
        ->Append(dec_to_double(&p->net_paid));
    static_cast<arrow::DoubleBuilder*>(builders["sr_ext_tax"].get())
        ->Append(dec_to_double(&p->ext_tax));
    static_cast<arrow::DoubleBuilder*>(builders["sr_net_paid_inc_tax"].get())
        ->Append(dec_to_double(&p->net_paid_inc_tax));
    static_cast<arrow::DoubleBuilder*>(builders["sr_fee"].get())
        ->Append(dec_to_double(&p->fee));
    static_cast<arrow::DoubleBuilder*>(builders["sr_ext_ship_cost"].get())
        ->Append(dec_to_double(&p->ext_ship_cost));
    static_cast<arrow::DoubleBuilder*>(builders["sr_refunded_cash"].get())
        ->Append(dec_to_double(&p->refunded_cash));
    static_cast<arrow::DoubleBuilder*>(builders["sr_reversed_charge"].get())
        ->Append(dec_to_double(&p->reversed_charge));
    static_cast<arrow::DoubleBuilder*>(builders["sr_store_credit"].get())
        ->Append(dec_to_double(&p->store_credit));
    static_cast<arrow::DoubleBuilder*>(builders["sr_net_loss"].get())
        ->Append(dec_to_double(&p->net_loss));
}

// ---------------------------------------------------------------------------
// catalog_returns
// ---------------------------------------------------------------------------

void append_catalog_returns_to_builders(
    const void* row,
    tpcds::BuilderMap& builders)
{
    auto* r = static_cast<const W_CATALOG_RETURNS_TBL*>(row);

    static_cast<arrow::Int64Builder*>(builders["cr_returned_date_sk"].get())
        ->Append(static_cast<int64_t>(r->cr_returned_date_sk));
    static_cast<arrow::Int64Builder*>(builders["cr_returned_time_sk"].get())
        ->Append(static_cast<int64_t>(r->cr_returned_time_sk));
    static_cast<arrow::Int64Builder*>(builders["cr_item_sk"].get())
        ->Append(static_cast<int64_t>(r->cr_item_sk));
    static_cast<arrow::Int64Builder*>(builders["cr_refunded_customer_sk"].get())
        ->Append(static_cast<int64_t>(r->cr_refunded_customer_sk));
    static_cast<arrow::Int64Builder*>(builders["cr_refunded_cdemo_sk"].get())
        ->Append(static_cast<int64_t>(r->cr_refunded_cdemo_sk));
    static_cast<arrow::Int64Builder*>(builders["cr_refunded_hdemo_sk"].get())
        ->Append(static_cast<int64_t>(r->cr_refunded_hdemo_sk));
    static_cast<arrow::Int64Builder*>(builders["cr_refunded_addr_sk"].get())
        ->Append(static_cast<int64_t>(r->cr_refunded_addr_sk));
    static_cast<arrow::Int64Builder*>(builders["cr_returning_customer_sk"].get())
        ->Append(static_cast<int64_t>(r->cr_returning_customer_sk));
    static_cast<arrow::Int64Builder*>(builders["cr_returning_cdemo_sk"].get())
        ->Append(static_cast<int64_t>(r->cr_returning_cdemo_sk));
    static_cast<arrow::Int64Builder*>(builders["cr_returning_hdemo_sk"].get())
        ->Append(static_cast<int64_t>(r->cr_returning_hdemo_sk));
    static_cast<arrow::Int64Builder*>(builders["cr_returning_addr_sk"].get())
        ->Append(static_cast<int64_t>(r->cr_returning_addr_sk));
    static_cast<arrow::Int64Builder*>(builders["cr_call_center_sk"].get())
        ->Append(static_cast<int64_t>(r->cr_call_center_sk));
    static_cast<arrow::Int64Builder*>(builders["cr_catalog_page_sk"].get())
        ->Append(static_cast<int64_t>(r->cr_catalog_page_sk));
    static_cast<arrow::Int64Builder*>(builders["cr_ship_mode_sk"].get())
        ->Append(static_cast<int64_t>(r->cr_ship_mode_sk));
    static_cast<arrow::Int64Builder*>(builders["cr_warehouse_sk"].get())
        ->Append(static_cast<int64_t>(r->cr_warehouse_sk));
    static_cast<arrow::Int64Builder*>(builders["cr_reason_sk"].get())
        ->Append(static_cast<int64_t>(r->cr_reason_sk));
    static_cast<arrow::Int64Builder*>(builders["cr_order_number"].get())
        ->Append(static_cast<int64_t>(r->cr_order_number));

    const ds_pricing_t* p = &r->cr_pricing;
    static_cast<arrow::Int32Builder*>(builders["cr_quantity"].get())
        ->Append(static_cast<int32_t>(p->quantity));
    static_cast<arrow::DoubleBuilder*>(builders["cr_net_paid"].get())
        ->Append(dec_to_double(&p->net_paid));
    static_cast<arrow::DoubleBuilder*>(builders["cr_ext_tax"].get())
        ->Append(dec_to_double(&p->ext_tax));
    static_cast<arrow::DoubleBuilder*>(builders["cr_net_paid_inc_tax"].get())
        ->Append(dec_to_double(&p->net_paid_inc_tax));
    static_cast<arrow::DoubleBuilder*>(builders["cr_fee"].get())
        ->Append(dec_to_double(&p->fee));
    static_cast<arrow::DoubleBuilder*>(builders["cr_ext_ship_cost"].get())
        ->Append(dec_to_double(&p->ext_ship_cost));
    static_cast<arrow::DoubleBuilder*>(builders["cr_refunded_cash"].get())
        ->Append(dec_to_double(&p->refunded_cash));
    static_cast<arrow::DoubleBuilder*>(builders["cr_reversed_charge"].get())
        ->Append(dec_to_double(&p->reversed_charge));
    static_cast<arrow::DoubleBuilder*>(builders["cr_store_credit"].get())
        ->Append(dec_to_double(&p->store_credit));
    static_cast<arrow::DoubleBuilder*>(builders["cr_net_loss"].get())
        ->Append(dec_to_double(&p->net_loss));
}

// ---------------------------------------------------------------------------
// web_returns
// ---------------------------------------------------------------------------

void append_web_returns_to_builders(
    const void* row,
    tpcds::BuilderMap& builders)
{
    auto* r = static_cast<const W_WEB_RETURNS_TBL*>(row);

    static_cast<arrow::Int64Builder*>(builders["wr_returned_date_sk"].get())
        ->Append(static_cast<int64_t>(r->wr_returned_date_sk));
    static_cast<arrow::Int64Builder*>(builders["wr_returned_time_sk"].get())
        ->Append(static_cast<int64_t>(r->wr_returned_time_sk));
    static_cast<arrow::Int64Builder*>(builders["wr_item_sk"].get())
        ->Append(static_cast<int64_t>(r->wr_item_sk));
    static_cast<arrow::Int64Builder*>(builders["wr_refunded_customer_sk"].get())
        ->Append(static_cast<int64_t>(r->wr_refunded_customer_sk));
    static_cast<arrow::Int64Builder*>(builders["wr_refunded_cdemo_sk"].get())
        ->Append(static_cast<int64_t>(r->wr_refunded_cdemo_sk));
    static_cast<arrow::Int64Builder*>(builders["wr_refunded_hdemo_sk"].get())
        ->Append(static_cast<int64_t>(r->wr_refunded_hdemo_sk));
    static_cast<arrow::Int64Builder*>(builders["wr_refunded_addr_sk"].get())
        ->Append(static_cast<int64_t>(r->wr_refunded_addr_sk));
    static_cast<arrow::Int64Builder*>(builders["wr_returning_customer_sk"].get())
        ->Append(static_cast<int64_t>(r->wr_returning_customer_sk));
    static_cast<arrow::Int64Builder*>(builders["wr_returning_cdemo_sk"].get())
        ->Append(static_cast<int64_t>(r->wr_returning_cdemo_sk));
    static_cast<arrow::Int64Builder*>(builders["wr_returning_hdemo_sk"].get())
        ->Append(static_cast<int64_t>(r->wr_returning_hdemo_sk));
    static_cast<arrow::Int64Builder*>(builders["wr_returning_addr_sk"].get())
        ->Append(static_cast<int64_t>(r->wr_returning_addr_sk));
    static_cast<arrow::Int64Builder*>(builders["wr_web_page_sk"].get())
        ->Append(static_cast<int64_t>(r->wr_web_page_sk));
    static_cast<arrow::Int64Builder*>(builders["wr_reason_sk"].get())
        ->Append(static_cast<int64_t>(r->wr_reason_sk));
    static_cast<arrow::Int64Builder*>(builders["wr_order_number"].get())
        ->Append(static_cast<int64_t>(r->wr_order_number));

    const ds_pricing_t* p = &r->wr_pricing;
    static_cast<arrow::Int32Builder*>(builders["wr_quantity"].get())
        ->Append(static_cast<int32_t>(p->quantity));
    static_cast<arrow::DoubleBuilder*>(builders["wr_net_paid"].get())
        ->Append(dec_to_double(&p->net_paid));
    static_cast<arrow::DoubleBuilder*>(builders["wr_ext_tax"].get())
        ->Append(dec_to_double(&p->ext_tax));
    static_cast<arrow::DoubleBuilder*>(builders["wr_net_paid_inc_tax"].get())
        ->Append(dec_to_double(&p->net_paid_inc_tax));
    static_cast<arrow::DoubleBuilder*>(builders["wr_fee"].get())
        ->Append(dec_to_double(&p->fee));
    static_cast<arrow::DoubleBuilder*>(builders["wr_ext_ship_cost"].get())
        ->Append(dec_to_double(&p->ext_ship_cost));
    static_cast<arrow::DoubleBuilder*>(builders["wr_refunded_cash"].get())
        ->Append(dec_to_double(&p->refunded_cash));
    static_cast<arrow::DoubleBuilder*>(builders["wr_reversed_charge"].get())
        ->Append(dec_to_double(&p->reversed_charge));
    static_cast<arrow::DoubleBuilder*>(builders["wr_store_credit"].get())
        ->Append(dec_to_double(&p->store_credit));
    static_cast<arrow::DoubleBuilder*>(builders["wr_net_loss"].get())
        ->Append(dec_to_double(&p->net_loss));
}

// ---------------------------------------------------------------------------
// Helper: append ds_addr_t fields with given column-name prefix
// ---------------------------------------------------------------------------
//
// prefix_street_number, prefix_street_name, prefix_street_type,
// prefix_suite_number, prefix_city, prefix_county, prefix_state,
// prefix_zip (as string), prefix_country, prefix_gmt_offset
//
static void append_addr_fields(
    const ds_addr_t& addr,
    const std::string& pfx,
    tpcds::BuilderMap& builders)
{
    static_cast<arrow::Int32Builder*>(builders[pfx + "street_number"].get())
        ->Append(addr.street_num);
    static_cast<arrow::StringBuilder*>(builders[pfx + "street_name"].get())
        ->Append(addr.street_name1 ? addr.street_name1 : "");
    static_cast<arrow::Int8Builder*>(builders[pfx + "street_type"].get())
        ->Append(encode_ca_street_type(addr.street_type ? addr.street_type : ""));
    static_cast<arrow::StringBuilder*>(builders[pfx + "suite_number"].get())
        ->Append(addr.suite_num);
    static_cast<arrow::StringBuilder*>(builders[pfx + "city"].get())
        ->Append(addr.city ? addr.city : "");
    static_cast<arrow::StringBuilder*>(builders[pfx + "county"].get())
        ->Append(addr.county ? addr.county : "");
    static_cast<arrow::Int8Builder*>(builders[pfx + "state"].get())
        ->Append(encode_state(addr.state ? addr.state : ""));
    char zip_buf[12];
    std::snprintf(zip_buf, sizeof(zip_buf), "%05d", addr.zip);
    static_cast<arrow::StringBuilder*>(builders[pfx + "zip"].get())
        ->Append(zip_buf);
    static_cast<arrow::Int8Builder*>(builders[pfx + "country"].get())
        ->Append(0);  // always "United States"
    static_cast<arrow::DoubleBuilder*>(builders[pfx + "gmt_offset"].get())
        ->Append(static_cast<double>(addr.gmt_offset));
}

// ---------------------------------------------------------------------------
// call_center
// ---------------------------------------------------------------------------

void append_call_center_to_builders(
    const void* row,
    tpcds::BuilderMap& builders)
{
    auto* r = static_cast<const struct CALL_CENTER_TBL*>(row);

    static_cast<arrow::Int64Builder*>(builders["cc_call_center_sk"].get())
        ->Append(static_cast<int64_t>(r->cc_call_center_sk));
    static_cast<arrow::StringBuilder*>(builders["cc_call_center_id"].get())
        ->Append(r->cc_call_center_id);
    static_cast<arrow::Int64Builder*>(builders["cc_rec_start_date_sk"].get())
        ->Append(static_cast<int64_t>(r->cc_rec_start_date_id));
    static_cast<arrow::Int64Builder*>(builders["cc_rec_end_date_sk"].get())
        ->Append(static_cast<int64_t>(r->cc_rec_end_date_id));
    static_cast<arrow::Int64Builder*>(builders["cc_closed_date_sk"].get())
        ->Append(static_cast<int64_t>(r->cc_closed_date_id));
    static_cast<arrow::Int64Builder*>(builders["cc_open_date_sk"].get())
        ->Append(static_cast<int64_t>(r->cc_open_date_id));
    static_cast<arrow::Int8Builder*>(builders["cc_name"].get())
        ->Append(encode_cc_name(r->cc_name ? r->cc_name : ""));
    static_cast<arrow::Int8Builder*>(builders["cc_class"].get())
        ->Append(encode_cc_class(r->cc_class ? r->cc_class : ""));
    static_cast<arrow::Int32Builder*>(builders["cc_employees"].get())
        ->Append(static_cast<int32_t>(r->cc_employees));
    static_cast<arrow::Int32Builder*>(builders["cc_sq_ft"].get())
        ->Append(static_cast<int32_t>(r->cc_sq_ft));
    static_cast<arrow::Int8Builder*>(builders["cc_hours"].get())
        ->Append(encode_cc_hours(r->cc_hours ? r->cc_hours : ""));
    static_cast<arrow::StringBuilder*>(builders["cc_manager"].get())
        ->Append(r->cc_manager);
    static_cast<arrow::Int32Builder*>(builders["cc_mkt_id"].get())
        ->Append(static_cast<int32_t>(r->cc_market_id));
    static_cast<arrow::StringBuilder*>(builders["cc_mkt_class"].get())
        ->Append(r->cc_market_class);
    static_cast<arrow::StringBuilder*>(builders["cc_mkt_desc"].get())
        ->Append(r->cc_market_desc);
    static_cast<arrow::StringBuilder*>(builders["cc_market_manager"].get())
        ->Append(r->cc_market_manager);
    static_cast<arrow::Int32Builder*>(builders["cc_division"].get())
        ->Append(static_cast<int32_t>(r->cc_division_id));
    static_cast<arrow::StringBuilder*>(builders["cc_division_name"].get())
        ->Append(r->cc_division_name);
    static_cast<arrow::Int32Builder*>(builders["cc_company"].get())
        ->Append(static_cast<int32_t>(r->cc_company));
    static_cast<arrow::StringBuilder*>(builders["cc_company_name"].get())
        ->Append(r->cc_company_name);
    append_addr_fields(r->cc_address, "cc_", builders);
    static_cast<arrow::DoubleBuilder*>(builders["cc_tax_percentage"].get())
        ->Append(dec_to_double(&r->cc_tax_percentage));
}

// ---------------------------------------------------------------------------
// catalog_page
// ---------------------------------------------------------------------------

void append_catalog_page_to_builders(
    const void* row,
    tpcds::BuilderMap& builders)
{
    auto* r = static_cast<const struct CATALOG_PAGE_TBL*>(row);

    static_cast<arrow::Int64Builder*>(builders["cp_catalog_page_sk"].get())
        ->Append(static_cast<int64_t>(r->cp_catalog_page_sk));
    static_cast<arrow::StringBuilder*>(builders["cp_catalog_page_id"].get())
        ->Append(r->cp_catalog_page_id);
    static_cast<arrow::Int64Builder*>(builders["cp_start_date_sk"].get())
        ->Append(static_cast<int64_t>(r->cp_start_date_id));
    static_cast<arrow::Int64Builder*>(builders["cp_end_date_sk"].get())
        ->Append(static_cast<int64_t>(r->cp_end_date_id));
    static_cast<arrow::Int8Builder*>(builders["cp_department"].get())
        ->Append(0);  // always "DEPARTMENT"
    static_cast<arrow::Int32Builder*>(builders["cp_catalog_number"].get())
        ->Append(static_cast<int32_t>(r->cp_catalog_number));
    static_cast<arrow::Int32Builder*>(builders["cp_catalog_page_number"].get())
        ->Append(static_cast<int32_t>(r->cp_catalog_page_number));
    static_cast<arrow::StringBuilder*>(builders["cp_description"].get())
        ->Append(r->cp_description);
    static_cast<arrow::Int8Builder*>(builders["cp_type"].get())
        ->Append(encode_cp_type(r->cp_type ? r->cp_type : ""));
}

// ---------------------------------------------------------------------------
// web_page
// ---------------------------------------------------------------------------

void append_web_page_to_builders(
    const void* row,
    tpcds::BuilderMap& builders)
{
    auto* r = static_cast<const struct W_WEB_PAGE_TBL*>(row);

    static_cast<arrow::Int64Builder*>(builders["wp_web_page_sk"].get())
        ->Append(static_cast<int64_t>(r->wp_page_sk));
    static_cast<arrow::StringBuilder*>(builders["wp_web_page_id"].get())
        ->Append(r->wp_page_id);
    static_cast<arrow::Int64Builder*>(builders["wp_rec_start_date_sk"].get())
        ->Append(static_cast<int64_t>(r->wp_rec_start_date_id));
    static_cast<arrow::Int64Builder*>(builders["wp_rec_end_date_sk"].get())
        ->Append(static_cast<int64_t>(r->wp_rec_end_date_id));
    static_cast<arrow::Int64Builder*>(builders["wp_creation_date_sk"].get())
        ->Append(static_cast<int64_t>(r->wp_creation_date_sk));
    static_cast<arrow::Int64Builder*>(builders["wp_access_date_sk"].get())
        ->Append(static_cast<int64_t>(r->wp_access_date_sk));
    static_cast<arrow::Int32Builder*>(builders["wp_autogen_flag"].get())
        ->Append(static_cast<int32_t>(r->wp_autogen_flag));
    static_cast<arrow::Int64Builder*>(builders["wp_customer_sk"].get())
        ->Append(static_cast<int64_t>(r->wp_customer_sk));
    static_cast<arrow::StringBuilder*>(builders["wp_url"].get())
        ->Append(r->wp_url);
    static_cast<arrow::Int8Builder*>(builders["wp_type"].get())
        ->Append(encode_wp_type(r->wp_type ? r->wp_type : ""));
    static_cast<arrow::Int32Builder*>(builders["wp_char_count"].get())
        ->Append(static_cast<int32_t>(r->wp_char_count));
    static_cast<arrow::Int32Builder*>(builders["wp_link_count"].get())
        ->Append(static_cast<int32_t>(r->wp_link_count));
    static_cast<arrow::Int32Builder*>(builders["wp_image_count"].get())
        ->Append(static_cast<int32_t>(r->wp_image_count));
    static_cast<arrow::Int32Builder*>(builders["wp_max_ad_count"].get())
        ->Append(static_cast<int32_t>(r->wp_max_ad_count));
}

// ---------------------------------------------------------------------------
// web_site
// ---------------------------------------------------------------------------

void append_web_site_to_builders(
    const void* row,
    tpcds::BuilderMap& builders)
{
    auto* r = static_cast<const struct W_WEB_SITE_TBL*>(row);

    static_cast<arrow::Int64Builder*>(builders["web_site_sk"].get())
        ->Append(static_cast<int64_t>(r->web_site_sk));
    static_cast<arrow::StringBuilder*>(builders["web_site_id"].get())
        ->Append(r->web_site_id);
    static_cast<arrow::Int64Builder*>(builders["web_rec_start_date_sk"].get())
        ->Append(static_cast<int64_t>(r->web_rec_start_date_id));
    static_cast<arrow::Int64Builder*>(builders["web_rec_end_date_sk"].get())
        ->Append(static_cast<int64_t>(r->web_rec_end_date_id));
    static_cast<arrow::StringBuilder*>(builders["web_name"].get())
        ->Append(r->web_name);
    static_cast<arrow::Int64Builder*>(builders["web_open_date_sk"].get())
        ->Append(static_cast<int64_t>(r->web_open_date));
    static_cast<arrow::Int64Builder*>(builders["web_close_date_sk"].get())
        ->Append(static_cast<int64_t>(r->web_close_date));
    static_cast<arrow::Int8Builder*>(builders["web_class"].get())
        ->Append(0);  // always "Unknown"
    static_cast<arrow::StringBuilder*>(builders["web_manager"].get())
        ->Append(r->web_manager);
    static_cast<arrow::Int32Builder*>(builders["web_mkt_id"].get())
        ->Append(static_cast<int32_t>(r->web_market_id));
    static_cast<arrow::StringBuilder*>(builders["web_mkt_class"].get())
        ->Append(r->web_market_class);
    static_cast<arrow::StringBuilder*>(builders["web_mkt_desc"].get())
        ->Append(r->web_market_desc);
    static_cast<arrow::StringBuilder*>(builders["web_market_manager"].get())
        ->Append(r->web_market_manager);
    static_cast<arrow::Int32Builder*>(builders["web_company_id"].get())
        ->Append(static_cast<int32_t>(r->web_company_id));
    static_cast<arrow::StringBuilder*>(builders["web_company_name"].get())
        ->Append(r->web_company_name);
    append_addr_fields(r->web_address, "web_", builders);
    static_cast<arrow::DoubleBuilder*>(builders["web_tax_percentage"].get())
        ->Append(dec_to_double(&r->web_tax_percentage));
}

// ---------------------------------------------------------------------------
// warehouse
// ---------------------------------------------------------------------------

void append_warehouse_to_builders(
    const void* row,
    tpcds::BuilderMap& builders)
{
    auto* r = static_cast<const struct W_WAREHOUSE_TBL*>(row);

    static_cast<arrow::Int64Builder*>(builders["w_warehouse_sk"].get())
        ->Append(static_cast<int64_t>(r->w_warehouse_sk));
    static_cast<arrow::StringBuilder*>(builders["w_warehouse_id"].get())
        ->Append(r->w_warehouse_id);
    static_cast<arrow::StringBuilder*>(builders["w_warehouse_name"].get())
        ->Append(r->w_warehouse_name);
    static_cast<arrow::Int32Builder*>(builders["w_warehouse_sq_ft"].get())
        ->Append(static_cast<int32_t>(r->w_warehouse_sq_ft));
    append_addr_fields(r->w_address, "w_", builders);
}

// ---------------------------------------------------------------------------
// ship_mode
// ---------------------------------------------------------------------------

void append_ship_mode_to_builders(
    const void* row,
    tpcds::BuilderMap& builders)
{
    auto* r = static_cast<const struct W_SHIP_MODE_TBL*>(row);

    static_cast<arrow::Int64Builder*>(builders["sm_ship_mode_sk"].get())
        ->Append(static_cast<int64_t>(r->sm_ship_mode_sk));
    static_cast<arrow::StringBuilder*>(builders["sm_ship_mode_id"].get())
        ->Append(r->sm_ship_mode_id);
    static_cast<arrow::Int8Builder*>(builders["sm_type"].get())
        ->Append(encode_sm_type(r->sm_type ? r->sm_type : ""));
    static_cast<arrow::Int8Builder*>(builders["sm_code"].get())
        ->Append(encode_sm_code(r->sm_code ? r->sm_code : ""));
    static_cast<arrow::Int8Builder*>(builders["sm_carrier"].get())
        ->Append(encode_sm_carrier(r->sm_carrier ? r->sm_carrier : ""));
    static_cast<arrow::StringBuilder*>(builders["sm_contract"].get())
        ->Append(r->sm_contract);
}

// ---------------------------------------------------------------------------
// household_demographics
// ---------------------------------------------------------------------------

void append_household_demographics_to_builders(
    const void* row,
    tpcds::BuilderMap& builders)
{
    auto* r = static_cast<const struct W_HOUSEHOLD_DEMOGRAPHICS_TBL*>(row);

    static_cast<arrow::Int64Builder*>(builders["hd_demo_sk"].get())
        ->Append(static_cast<int64_t>(r->hd_demo_sk));
    static_cast<arrow::Int64Builder*>(builders["hd_income_band_sk"].get())
        ->Append(static_cast<int64_t>(r->hd_income_band_id));
    static_cast<arrow::StringBuilder*>(builders["hd_buy_potential"].get())
        ->Append(r->hd_buy_potential ? r->hd_buy_potential : "");
    static_cast<arrow::Int32Builder*>(builders["hd_dep_count"].get())
        ->Append(static_cast<int32_t>(r->hd_dep_count));
    static_cast<arrow::Int32Builder*>(builders["hd_vehicle_count"].get())
        ->Append(static_cast<int32_t>(r->hd_vehicle_count));
}

// ---------------------------------------------------------------------------
// customer_demographics
// ---------------------------------------------------------------------------

void append_customer_demographics_to_builders(
    const void* row,
    tpcds::BuilderMap& builders)
{
    auto* r = static_cast<const struct W_CUSTOMER_DEMOGRAPHICS_TBL*>(row);

    static_cast<arrow::Int64Builder*>(builders["cd_demo_sk"].get())
        ->Append(static_cast<int64_t>(r->cd_demo_sk));
    static_cast<arrow::Int8Builder*>(builders["cd_gender"].get())
        ->Append(encode_cd_gender(r->cd_gender ? r->cd_gender : ""));
    static_cast<arrow::Int8Builder*>(builders["cd_marital_status"].get())
        ->Append(encode_cd_marital_status(r->cd_marital_status ? r->cd_marital_status : ""));
    static_cast<arrow::Int8Builder*>(builders["cd_education_status"].get())
        ->Append(encode_cd_education_status(r->cd_education_status ? r->cd_education_status : ""));
    static_cast<arrow::Int32Builder*>(builders["cd_purchase_estimate"].get())
        ->Append(static_cast<int32_t>(r->cd_purchase_estimate));
    static_cast<arrow::Int8Builder*>(builders["cd_credit_rating"].get())
        ->Append(encode_cd_credit_rating(r->cd_credit_rating ? r->cd_credit_rating : ""));
    static_cast<arrow::Int32Builder*>(builders["cd_dep_count"].get())
        ->Append(static_cast<int32_t>(r->cd_dep_count));
    static_cast<arrow::Int32Builder*>(builders["cd_dep_employed_count"].get())
        ->Append(static_cast<int32_t>(r->cd_dep_employed_count));
    static_cast<arrow::Int32Builder*>(builders["cd_dep_college_count"].get())
        ->Append(static_cast<int32_t>(r->cd_dep_college_count));
}

// ---------------------------------------------------------------------------
// customer_address
// ---------------------------------------------------------------------------

void append_customer_address_to_builders(
    const void* row,
    tpcds::BuilderMap& builders)
{
    auto* r = static_cast<const struct W_CUSTOMER_ADDRESS_TBL*>(row);

    static_cast<arrow::Int64Builder*>(builders["ca_address_sk"].get())
        ->Append(static_cast<int64_t>(r->ca_addr_sk));
    static_cast<arrow::StringBuilder*>(builders["ca_address_id"].get())
        ->Append(r->ca_addr_id);
    append_addr_fields(r->ca_address, "ca_", builders);
    static_cast<arrow::Int8Builder*>(builders["ca_location_type"].get())
        ->Append(encode_ca_location_type(r->ca_location_type ? r->ca_location_type : ""));
}

// ---------------------------------------------------------------------------
// income_band
// ---------------------------------------------------------------------------

void append_income_band_to_builders(
    const void* row,
    tpcds::BuilderMap& builders)
{
    auto* r = static_cast<const struct W_INCOME_BAND_TBL*>(row);

    static_cast<arrow::Int32Builder*>(builders["ib_income_band_id"].get())
        ->Append(static_cast<int32_t>(r->ib_income_band_id));
    static_cast<arrow::Int32Builder*>(builders["ib_lower_bound"].get())
        ->Append(static_cast<int32_t>(r->ib_lower_bound));
    static_cast<arrow::Int32Builder*>(builders["ib_upper_bound"].get())
        ->Append(static_cast<int32_t>(r->ib_upper_bound));
}

// ---------------------------------------------------------------------------
// reason
// ---------------------------------------------------------------------------

void append_reason_to_builders(
    const void* row,
    tpcds::BuilderMap& builders)
{
    auto* r = static_cast<const struct W_REASON_TBL*>(row);

    static_cast<arrow::Int64Builder*>(builders["r_reason_sk"].get())
        ->Append(static_cast<int64_t>(r->r_reason_sk));
    static_cast<arrow::StringBuilder*>(builders["r_reason_id"].get())
        ->Append(r->r_reason_id);
    static_cast<arrow::StringBuilder*>(builders["r_reason_desc"].get())
        ->Append(r->r_reason_description ? r->r_reason_description : "");
}

// ---------------------------------------------------------------------------
// time_dim
// ---------------------------------------------------------------------------

void append_time_dim_to_builders(
    const void* row,
    tpcds::BuilderMap& builders)
{
    auto* r = static_cast<const struct W_TIME_TBL*>(row);

    static_cast<arrow::Int64Builder*>(builders["t_time_sk"].get())
        ->Append(static_cast<int64_t>(r->t_time_sk));
    static_cast<arrow::StringBuilder*>(builders["t_time_id"].get())
        ->Append(r->t_time_id);
    static_cast<arrow::Int32Builder*>(builders["t_time"].get())
        ->Append(static_cast<int32_t>(r->t_time));
    static_cast<arrow::Int32Builder*>(builders["t_hour"].get())
        ->Append(static_cast<int32_t>(r->t_hour));
    static_cast<arrow::Int32Builder*>(builders["t_minute"].get())
        ->Append(static_cast<int32_t>(r->t_minute));
    static_cast<arrow::Int32Builder*>(builders["t_second"].get())
        ->Append(static_cast<int32_t>(r->t_second));
    static_cast<arrow::Int8Builder*>(builders["t_am_pm"].get())
        ->Append(encode_t_am_pm(r->t_am_pm ? r->t_am_pm : ""));
    static_cast<arrow::Int8Builder*>(builders["t_shift"].get())
        ->Append(encode_t_shift(r->t_shift ? r->t_shift : ""));
    static_cast<arrow::Int8Builder*>(builders["t_sub_shift"].get())
        ->Append(encode_t_sub_shift(r->t_sub_shift ? r->t_sub_shift : ""));
    static_cast<arrow::Int8Builder*>(builders["t_meal_time"].get())
        ->Append(encode_t_meal_time(r->t_meal_time ? r->t_meal_time : ""));
}

// ---------------------------------------------------------------------------
// promotion
// ---------------------------------------------------------------------------

void append_promotion_to_builders(
    const void* row,
    tpcds::BuilderMap& builders)
{
    auto* r = static_cast<const struct W_PROMOTION_TBL*>(row);

    static_cast<arrow::Int64Builder*>(builders["p_promo_sk"].get())
        ->Append(static_cast<int64_t>(r->p_promo_sk));
    static_cast<arrow::StringBuilder*>(builders["p_promo_id"].get())
        ->Append(r->p_promo_id);
    static_cast<arrow::Int64Builder*>(builders["p_start_date_sk"].get())
        ->Append(static_cast<int64_t>(r->p_start_date_id));
    static_cast<arrow::Int64Builder*>(builders["p_end_date_sk"].get())
        ->Append(static_cast<int64_t>(r->p_end_date_id));
    static_cast<arrow::Int64Builder*>(builders["p_item_sk"].get())
        ->Append(static_cast<int64_t>(r->p_item_sk));
    static_cast<arrow::DoubleBuilder*>(builders["p_cost"].get())
        ->Append(dec_to_double(&r->p_cost));
    static_cast<arrow::Int32Builder*>(builders["p_response_target"].get())
        ->Append(static_cast<int32_t>(r->p_response_target));
    static_cast<arrow::StringBuilder*>(builders["p_promo_name"].get())
        ->Append(r->p_promo_name);
    static_cast<arrow::Int32Builder*>(builders["p_channel_dmail"].get())
        ->Append(static_cast<int32_t>(r->p_channel_dmail));
    static_cast<arrow::Int32Builder*>(builders["p_channel_email"].get())
        ->Append(static_cast<int32_t>(r->p_channel_email));
    static_cast<arrow::Int32Builder*>(builders["p_channel_catalog"].get())
        ->Append(static_cast<int32_t>(r->p_channel_catalog));
    static_cast<arrow::Int32Builder*>(builders["p_channel_tv"].get())
        ->Append(static_cast<int32_t>(r->p_channel_tv));
    static_cast<arrow::Int32Builder*>(builders["p_channel_radio"].get())
        ->Append(static_cast<int32_t>(r->p_channel_radio));
    static_cast<arrow::Int32Builder*>(builders["p_channel_press"].get())
        ->Append(static_cast<int32_t>(r->p_channel_press));
    static_cast<arrow::Int32Builder*>(builders["p_channel_event"].get())
        ->Append(static_cast<int32_t>(r->p_channel_event));
    static_cast<arrow::Int32Builder*>(builders["p_channel_demo"].get())
        ->Append(static_cast<int32_t>(r->p_channel_demo));
    static_cast<arrow::StringBuilder*>(builders["p_channel_details"].get())
        ->Append(r->p_channel_details);
    static_cast<arrow::Int8Builder*>(builders["p_purpose"].get())
        ->Append(0);  // always "Unknown"
    static_cast<arrow::Int32Builder*>(builders["p_discount_active"].get())
        ->Append(static_cast<int32_t>(r->p_discount_active));
}

// ---------------------------------------------------------------------------
// store
// ---------------------------------------------------------------------------

void append_store_to_builders(
    const void* row,
    tpcds::BuilderMap& builders)
{
    auto* r = static_cast<const struct W_STORE_TBL*>(row);

    static_cast<arrow::Int64Builder*>(builders["s_store_sk"].get())
        ->Append(static_cast<int64_t>(r->store_sk));
    static_cast<arrow::StringBuilder*>(builders["s_store_id"].get())
        ->Append(r->store_id);
    static_cast<arrow::Int64Builder*>(builders["s_rec_start_date"].get())
        ->Append(static_cast<int64_t>(r->rec_start_date_id));
    static_cast<arrow::Int64Builder*>(builders["s_rec_end_date"].get())
        ->Append(static_cast<int64_t>(r->rec_end_date_id));
    static_cast<arrow::Int64Builder*>(builders["s_closed_date_sk"].get())
        ->Append(static_cast<int64_t>(r->closed_date_id));
    static_cast<arrow::StringBuilder*>(builders["s_store_name"].get())
        ->Append(r->store_name);
    static_cast<arrow::Int32Builder*>(builders["s_number_employees"].get())
        ->Append(static_cast<int32_t>(r->employees));
    static_cast<arrow::Int32Builder*>(builders["s_floor_space"].get())
        ->Append(static_cast<int32_t>(r->floor_space));
    static_cast<arrow::Int8Builder*>(builders["s_hours"].get())
        ->Append(encode_cc_hours(r->hours ? r->hours : ""));
    static_cast<arrow::StringBuilder*>(builders["s_manager"].get())
        ->Append(r->store_manager);
    static_cast<arrow::Int32Builder*>(builders["s_market_id"].get())
        ->Append(static_cast<int32_t>(r->market_id));
    static_cast<arrow::Int8Builder*>(builders["s_geography_class"].get())
        ->Append(0);  // always "Unknown"
    static_cast<arrow::StringBuilder*>(builders["s_market_desc"].get())
        ->Append(r->market_desc);
    static_cast<arrow::StringBuilder*>(builders["s_market_manager"].get())
        ->Append(r->market_manager);
    static_cast<arrow::Int64Builder*>(builders["s_division_id"].get())
        ->Append(static_cast<int64_t>(r->division_id));
    static_cast<arrow::Int8Builder*>(builders["s_division_name"].get())
        ->Append(0);  // always "Unknown"
    static_cast<arrow::Int64Builder*>(builders["s_company_id"].get())
        ->Append(static_cast<int64_t>(r->company_id));
    static_cast<arrow::Int8Builder*>(builders["s_company_name"].get())
        ->Append(0);  // always "Unknown"
    append_addr_fields(r->address, "s_", builders);
    static_cast<arrow::DoubleBuilder*>(builders["s_tax_percentage"].get())
        ->Append(dec_to_double(&r->dTaxPercentage));
}

// ---------------------------------------------------------------------------
// Generic dispatcher
// ---------------------------------------------------------------------------

void append_dsdgen_row_to_builders(
    const std::string& tbl_name,
    const void* row,
    tpcds::BuilderMap& builders)
{
    if (tbl_name == "store_sales") {
        append_store_sales_to_builders(row, builders);
    } else if (tbl_name == "inventory") {
        append_inventory_to_builders(row, builders);
    } else if (tbl_name == "catalog_sales") {
        append_catalog_sales_to_builders(row, builders);
    } else if (tbl_name == "web_sales") {
        append_web_sales_to_builders(row, builders);
    } else if (tbl_name == "customer") {
        append_customer_to_builders(row, builders);
    } else if (tbl_name == "item") {
        append_item_to_builders(row, builders);
    } else if (tbl_name == "date_dim") {
        append_date_dim_to_builders(row, builders);
    } else if (tbl_name == "store_returns") {
        append_store_returns_to_builders(row, builders);
    } else if (tbl_name == "catalog_returns") {
        append_catalog_returns_to_builders(row, builders);
    } else if (tbl_name == "web_returns") {
        append_web_returns_to_builders(row, builders);
    } else if (tbl_name == "call_center") {
        append_call_center_to_builders(row, builders);
    } else if (tbl_name == "catalog_page") {
        append_catalog_page_to_builders(row, builders);
    } else if (tbl_name == "web_page") {
        append_web_page_to_builders(row, builders);
    } else if (tbl_name == "web_site") {
        append_web_site_to_builders(row, builders);
    } else if (tbl_name == "warehouse") {
        append_warehouse_to_builders(row, builders);
    } else if (tbl_name == "ship_mode") {
        append_ship_mode_to_builders(row, builders);
    } else if (tbl_name == "household_demographics") {
        append_household_demographics_to_builders(row, builders);
    } else if (tbl_name == "customer_demographics") {
        append_customer_demographics_to_builders(row, builders);
    } else if (tbl_name == "customer_address") {
        append_customer_address_to_builders(row, builders);
    } else if (tbl_name == "income_band") {
        append_income_band_to_builders(row, builders);
    } else if (tbl_name == "reason") {
        append_reason_to_builders(row, builders);
    } else if (tbl_name == "time_dim") {
        append_time_dim_to_builders(row, builders);
    } else if (tbl_name == "promotion") {
        append_promotion_to_builders(row, builders);
    } else if (tbl_name == "store") {
        append_store_to_builders(row, builders);
    } else {
        throw std::invalid_argument("append_dsdgen_row_to_builders: unknown table: " + tbl_name);
    }
}

}  // namespace tpcds
