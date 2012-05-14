
-----------------------------------------------------

-- Store Bid
-- minBid, bid, maxBid???
-- * Assume that bid type 0 = bid, 1 = buy now
UPDATE ITEM SET i_max_bid=$maxBid, i_nb_of_bids = i_nb_of_bids+1 WHERE i_id=$itemId;

INSERT INTO ITEM_BID(ib_id, ib_i_id, ib_u_id, ib_buyer_id, ib_quantity, ib_type, ib_bid, ib_max_bid, ib_date) 
VALUES ($bidId, $itemId, $sellerId, $buyerId,  $qty, 0, $bid, $maxBid, '$now');

-----------------------------------------------------

-- Check win bid : Scheduled stored proc?
-- Question: How do we "trigger" some event at the end of the auction? (i.e., close auction, update winner info, etc)
-- 
-- Thoughts:
-- All auctions end at the start of a minute. There is a single-sited procedure that gets fired off at regular intervals
-- to do a full table scan and return a list of the item ids whose auction has ended. The benchmark client then
-- fires off a new transaction for each item that is sold.

-- This transaction is executed on the sellers' partoitions

-----------------------------------------------------

-- Process End of Auction
-- This transaction is executed on the buyers' partitions
-- Update USER_ITEMS

-----------------------------------------------------

-- Store Buy Now
SET $oldQty = (SELECT i_quantity FROM ITEM WHERE i_id=$itemId);
SET $newQty = $oldQty - $qty;
IF $newQty == 0 
	UPDATE ITEM SET i_end_date=NOW(), i_quantity=$newQty WHERE i_id=$itemId;
ELSE
	UPDATE ITEM SET i_quantity=$newQty WHERE i_id=$itemId;
END
INSERT INTO ITEM_BID(ib_id, ib_i_id, ib_u_id, ib_buyer_id, ib_quantity, ib_type, ib_bid, ib_max_bid, ib_date) 
VALUES ($bidId, $itemId, $sellerId, $buyerId,  $qty, 1, $bid, $bid, '$now');

INSERT INTO ITEM_PURCHASE(ip_id, ip_ib_id, ip_i_id, ip_u_id, ip_date) 
VALUES ($purchaseId, $bidId, $itemId, $sellerId, '$now');

-----------------------------------------------------

-- Store Comment
UPDATE USER SET u_rating=$rating WHERE u_id=$to;

INSERT INTO ITEM_FEEDBACK(if_id, if_i_id, if_u_id, if_buyer_id, if_rating, if_date, if_comment) 
VALUES ($feedbackId, $itemId, $sellerId, $buyerId, $rating, '$now', \"$comment\");

-----------------------------------------------------

-- Create New Item
INSERT INTO ITEM(i_id, i_u_id, i_c_id, i_name, i_description, i_initial_price, i_quantity, i_reserve_price, i_buy_now, i_nb_of_bids, i_max_bid, i_user_attributes, i_start_date, i_end_date) 
VALUES ($itemId, $sellerId, $categoryId, \"$name\", \"$description\", $initialPrice, $quantity, $reservePrice, $buyNow, 0, 0, \"$userAttributes\", '$start', '$end');

LOOP {
	-- Insert into attributeGroup and attributeValue too?
	INSERT INTO ITEM_ATTRIBUTE(ia_id, ia_i_id, ia_u_id, ia_gav_id) 
	VALUES($itemAttributeId, $itemId, $sellerId, $attributeValueId);
}

-- Possibly update the user attributes
LOOP {
      -- INSERT or UPDATE?
	INSERT INTO USER_ATTRIBUTES(ua_id, ua_u_id, ua_name, ua_value, u_created) 
	VALUES($userAttributeId, $userId, $name, $value, '$now');
}

-----------------------------------------------------

-- Item comment (Q&A)
-- * Don't forget to put sellerId for partitioning
-- Buyer 
	INSERT INTO ITEM_COMMENT(ic_id, ic_i_id, ic_u_id, ic_date, ic_question) 
	VALUES($commentId, $itemId, $buyerId, '$now', $question);
-- Seller
	UPDATE ITEM_COMMENT SET ic_respones = $response 
	WHERE ic_id = $commentId;

-----------------------------------------------------

-- Create new user
INSERT INTO USER(u_id, u_firstname, u_lastname, u_password, u_email, u_rating, u_balance, u_created, u_r_id) 
VALUES($userId, $firstName, $lastName, $password, $email, 0, 0, '$now', $regionId);

LOOP {
	INSERT INTO USER_ATTRIBUTES(ua_id, ua_u_id, ua_name, ua_value, u_created) 
	VALUES($userAttributeId, $userId, $name, $value, '$now');
}

-----------------------------------------------------

-- Store Feedback
	INSERT INTO ITEM_FEEDBACK(....

-----------------------------------------------------