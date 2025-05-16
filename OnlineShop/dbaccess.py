import mysql.connector

DB_CONFIG = {
    'host': '10.11.5.5',
    'user': 'shopuser1',
    'password': 'password1',
    'database': 'onlineshop'
}

# Global connection and cursor
_conn = None
_cur = None

def init_connection():
    global _conn, _cur
    if _conn is None or not _conn.is_connected():
        _conn = mysql.connector.connect(**DB_CONFIG)
        _cur = _conn.cursor(buffered=True)  # buffered cursor to allow multiple fetches
    return _conn, _cur

def close_connection():
    global _conn, _cur
    if _cur is not None:
        _cur.close()
        _cur = None
    if _conn is not None:
        _conn.close()
        _conn = None

def gen_custID():
    conn, cur = init_connection()
    cur.execute("UPDATE metadata SET custnum = custnum + 1")
    conn.commit()
    cur.execute("SELECT custnum FROM metadata")
    custnum = str(cur.fetchone()[0])
    id = "CID" + "0"*(7-len(custnum)) + custnum
    return id

def gen_sellID():
    conn, cur = init_connection()
    cur.execute("UPDATE metadata SET sellnum = sellnum + 1")
    conn.commit()
    cur.execute("SELECT sellnum FROM metadata")
    sellnum = str(cur.fetchone()[0])
    id = "SID" + "0"*(7-len(sellnum)) + sellnum
    return id

def gen_prodID():
    conn, cur = init_connection()
    cur.execute("UPDATE metadata SET prodnum = prodnum + 1")
    conn.commit()
    cur.execute("SELECT prodnum FROM metadata")
    prodnum = str(cur.fetchone()[0])
    id = "PID" + "0"*(7-len(prodnum)) + prodnum
    return id

def gen_orderID():
    conn, cur = init_connection()
    cur.execute("UPDATE metadata SET ordernum = ordernum + 1")
    conn.commit()
    cur.execute("SELECT ordernum FROM metadata")
    ordernum = str(cur.fetchone()[0])
    id = "OID" + "0"*(7-len(ordernum)) + ordernum
    return id

def add_user(data):
    conn, cur = init_connection()
    email = data["email"]
    if data['type'] == "Customer":
        cur.execute("SELECT * FROM customer WHERE email=%s", (email,))
    elif data['type'] == "Seller":
        cur.execute("SELECT * FROM seller WHERE email=%s", (email,))
    if cur.fetchone() is not None:
        return False

    tup = (data["name"], data["email"], data["phone"], data["area"], data["locality"],
           data["city"], data["state"], data["country"], data["zip"], data["password"])

    if data['type'] == "Customer":
        cur.execute("INSERT INTO customer VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", 
                    (gen_custID(), *tup))
    elif data['type'] == "Seller":
        cur.execute("INSERT INTO seller VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", 
                    (gen_sellID(), *tup))
    conn.commit()
    return True

def auth_user(data):
    conn, cur = init_connection()
    type_ = data["type"]
    email = data["email"]
    password = data["password"]
    if type_ == "Customer":
        cur.execute("SELECT custID, name FROM customer WHERE email=%s AND password=%s", (email, password))
    elif type_ == "Seller":
        cur.execute("SELECT sellID, name FROM seller WHERE email=%s AND password=%s", (email, password))
    res = cur.fetchone()
    if res is None:
        return False
    return res

def fetch_details(userid, type_):
    conn, cur = init_connection()
    if type_ == "Customer":
        cur.execute("SELECT * FROM customer WHERE custID=%s", (userid,))
        a = cur.fetchall()
        b = []
    elif type_ == "Seller":
        cur.execute("SELECT * FROM seller WHERE sellID=%s", (userid,))
        a = cur.fetchall()
        cur.execute("SELECT DISTINCT(category) FROM product WHERE sellID=%s", (userid,))
        b = [i[0] for i in cur.fetchall()]
    return a, b

def search_users(search, srch_type):
    conn, cur = init_connection()
    search = "%" + search.lower() + "%"
    if srch_type == "Customer":
        cur.execute("SELECT custID, name, email, phone, area, locality, city, state, country, zipcode FROM customer WHERE LOWER(name) LIKE %s", (search,))
    elif srch_type == "Seller":
        cur.execute("SELECT sellID, name, email, phone, area, locality, city, state, country, zipcode FROM seller WHERE LOWER(name) LIKE %s", (search,))
    res = cur.fetchall()
    return res

def update_details(data, userid, type_):
    conn, cur = init_connection()
    if type_ == "Customer":
        cur.execute("""UPDATE customer SET phone=%s, area=%s, locality=%s, city=%s, state=%s, country=%s, zipcode=%s WHERE custID=%s""",
                    (data["phone"], data["area"], data["locality"], data["city"], data["state"], data["country"], data["zip"], userid))
    elif type_ == "Seller":
        cur.execute("""UPDATE seller SET phone=%s, area=%s, locality=%s, city=%s, state=%s, country=%s, zipcode=%s WHERE sellID=%s""",
                    (data["phone"], data["area"], data["locality"], data["city"], data["state"], data["country"], data["zip"], userid))
    conn.commit()

def check_psswd(psswd, userid, type_):
    conn, cur = init_connection()
    if type_ == "Customer":
        cur.execute("SELECT password FROM customer WHERE custID=%s", (userid,))
    elif type_ == "Seller":
        cur.execute("SELECT password FROM seller WHERE sellID=%s", (userid,))
    real_psswd = cur.fetchone()[0]
    return psswd == real_psswd

def set_psswd(psswd, userid, type_):
    conn, cur = init_connection()
    if type_ == "Customer":
        cur.execute("UPDATE customer SET password=%s WHERE custID=%s", (psswd, userid))
    elif type_ == "Seller":
        cur.execute("UPDATE seller SET password=%s WHERE sellID=%s", (psswd, userid))
    conn.commit()

def add_prod(sellID, data):
    conn, cur = init_connection()
    prodID = gen_prodID()
    cur.execute("INSERT INTO product VALUES (%s, %s, %s, %s, %s, (SELECT profit_rate FROM metadata), %s, %s)",
                (prodID, data["name"], data["qty"], data["category"], data["price"], data["price"], data["desp"], sellID))
    conn.commit()

def get_categories(sellID):
    conn, cur = init_connection()
    cur.execute("SELECT DISTINCT(category) FROM product WHERE sellID=%s", (sellID,))
    categories = [i[0] for i in cur.fetchall()]
    return categories

def search_myproduct(sellID, srchBy, category, keyword):
    conn, cur = init_connection()
    keywords = ['%' + i + '%' for i in keyword.split()]
    if not keywords:
        keywords = ['%%']
    res = []
    if srchBy == "by category":
        cur.execute("SELECT prodID, name, quantity, category, cost_price FROM product WHERE category=%s AND sellID=%s", (category, sellID))
        res = cur.fetchall()
    elif srchBy == "by keyword":
        for word in keywords:
            cur.execute("""SELECT prodID, name, quantity, category, cost_price FROM product
                           WHERE (name LIKE %s OR description LIKE %s OR category LIKE %s) AND sellID=%s""",
                        (word, word, word, sellID))
            res.extend(cur.fetchall())
        res = list(set(res))
    elif srchBy == "both":
        for word in keywords:
            cur.execute("""SELECT prodID, name, quantity, category, cost_price FROM product
                           WHERE (name LIKE %s OR description LIKE %s) AND sellID=%s AND category=%s""",
                        (word, word, sellID, category))
            res.extend(cur.fetchall())
        res = list(set(res))
    return res

def get_product_info(id):
    conn, cur = init_connection()
    cur.execute("""SELECT p.name, p.quantity, p.category, p.cost_price, p.sell_price,
                          p.sellID, p.description, s.name FROM product p JOIN seller s 
                  ON p.sellID = s.sellID WHERE p.prodID=%s""", (id,))
    res = cur.fetchall()
    if len(res) == 0:
        return False, res
    return True, res[0]


def update_product(data, id):
    conn, cur = init_connection()
    cur.execute("""UPDATE product SET name=%s, quantity=%s, category=%s, cost_price=%s, sell_price=%s, description=%s
                   WHERE prodID=%s""",
                (data["name"], data["qty"], data["category"], data["price"], data["price"], data["desp"], id))
    conn.commit()

def search_products(srchBy, category, keyword):
    conn, cur = init_connection()
    keywords = ['%' + i + '%' for i in keyword.split()]
    if len(keywords) == 0:
        keywords.append('%%')
    res = []
    if srchBy == "by category":
        cur.execute("SELECT prodID, name, quantity, category, cost_price FROM product WHERE category=%s", (category,))
        res = cur.fetchall()
    elif srchBy == "by keyword":
        for word in keywords:
            cur.execute("""SELECT prodID, name, quantity, category, cost_price FROM product
                           WHERE name LIKE %s OR description LIKE %s OR category LIKE %s""",
                        (word, word, word))
            res.extend(cur.fetchall())
        res = list(set(res))
    elif srchBy == "both":
        for word in keywords:
            cur.execute("""SELECT prodID, name, quantity, category, cost_price FROM product
                           WHERE (name LIKE %s OR description LIKE %s) AND category=%s""",
                        (word, word, category))
            res.extend(cur.fetchall())
        res = list(set(res))
    return res

def get_seller_products(sellID):
    conn, cur = init_connection()
    cur.execute("SELECT prodID, name, quantity, category, cost_price FROM product WHERE sellID=%s", (sellID,))
    return cur.fetchall()

def place_order(prodID, custID, qty):
    conn, cur = init_connection()
    orderID = gen_orderID()
    # Insert order record with status 'Pending' (example)
    cur.execute("INSERT INTO orders VALUES (%s, %s, %s, %s, %s, %s)", (orderID, prodID, custID, qty, 'Pending', None))
    # Update product quantity
    cur.execute("UPDATE product SET quantity = quantity - %s WHERE prodID = %s", (qty, prodID))
    conn.commit()
    return orderID

def cust_orders(custID):
    conn, cur = init_connection()
    cur.execute("SELECT * FROM orders WHERE custID=%s", (custID,))
    return cur.fetchall()

def sell_orders(sellID):
    conn, cur = init_connection()
    cur.execute("""SELECT o.* FROM orders o JOIN product p ON o.prodID = p.prodID
                   WHERE p.sellID=%s""", (sellID,))
    return cur.fetchall()

def get_order_details(orderID):
    conn, cur = init_connection()
    cur.execute("SELECT * FROM orders WHERE orderID=%s", (orderID,))
    return cur.fetchone()

def change_order_status(orderID, new_status):
    conn, cur = init_connection()
    cur.execute("UPDATE orders SET status=%s WHERE orderID=%s", (new_status, orderID))
    conn.commit()

def cust_purchases(custID):
    conn, cur = init_connection()
    cur.execute("""SELECT o.*, p.name, p.category, p.sell_price FROM orders o
                   JOIN product p ON o.prodID = p.prodID
                   WHERE o.custID=%s AND o.status='Completed'""", (custID,))
    return cur.fetchall()

def sell_sales(sellID):
    conn, cur = init_connection()
    cur.execute("""SELECT o.*, p.name, p.category, p.sell_price FROM orders o
                   JOIN product p ON o.prodID = p.prodID
                   WHERE p.sellID=%s AND o.status='Completed'""", (sellID,))
    return cur.fetchall()

def add_product_to_cart(prodID, custID):
    conn, cur = init_connection()
    # Check if product already in cart
    cur.execute("SELECT quantity FROM cart WHERE custID=%s AND prodID=%s", (custID, prodID))
    row = cur.fetchone()
    if row is None:
        cur.execute("INSERT INTO cart VALUES (%s, %s, %s)", (custID, prodID, 1))
    else:
        cur.execute("UPDATE cart SET quantity = quantity + 1 WHERE custID=%s AND prodID=%s", (custID, prodID))
    conn.commit()

def get_cart(custID):
    conn, cur = init_connection()
    cur.execute("""SELECT c.prodID, c.quantity, p.name, p.category, p.sell_price FROM cart c
                   JOIN product p ON c.prodID = p.prodID WHERE c.custID=%s""", (custID,))
    return cur.fetchall()

def update_cart(custID, prodID, qty):
    conn, cur = init_connection()
    if qty <= 0:
        cur.execute("DELETE FROM cart WHERE custID=%s AND prodID=%s", (custID, prodID))
    else:
        cur.execute("UPDATE cart SET quantity=%s WHERE custID=%s AND prodID=%s", (qty, custID, prodID))
    conn.commit()

def cart_purchase(custID):
    conn, cur = init_connection()
    # Get cart items
    cur.execute("SELECT prodID, quantity FROM cart WHERE custID=%s", (custID,))
    items = cur.fetchall()
    order_ids = []
    for prodID, qty in items:
        orderID = gen_orderID()
        cur.execute("INSERT INTO orders VALUES (%s, %s, %s, %s, %s, %s)", (orderID, prodID, custID, qty, 'Pending', None))
        cur.execute("UPDATE product SET quantity = quantity - %s WHERE prodID = %s", (qty, prodID))
        order_ids.append(orderID)
    # Empty cart
    cur.execute("DELETE FROM cart WHERE custID=%s", (custID,))
    conn.commit()
    return order_ids

def empty_cart(custID):
    conn, cur = init_connection()
    cur.execute("DELETE FROM cart WHERE custID=%s", (custID,))
    conn.commit()

def remove_from_cart(custID, prodID):
    conn, cur = init_connection()
    cur.execute("DELETE FROM cart WHERE custID=%s AND prodID=%s", (custID, prodID))
    conn.commit()

